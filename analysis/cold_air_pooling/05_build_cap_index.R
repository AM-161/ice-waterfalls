suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(terra)
})

terraOptions(progress = 0)

# Build cached cold-air-pooling (CAP) potential values for icefalls and
# their assigned meteorological stations. The operational model reads the
# resulting CSVs, so CAP is not recomputed for every UID plot.
# By default only stations used by icefalls_nearest_station.csv are processed;
# pass --all-stations or CAP_ALL_STATIONS=1 to score the full stations table.

PATH_ASSIGN <- "data/AWS/icefalls_nearest_station.csv"
PATH_STATIONS <- "data/AWS/stations_all.csv"
PATH_GLACIERS <- Sys.getenv("ICEFALL_GLACIER_SHP", "adj_model/AWS Validation/GI_4_2015.shp")

OUT_DIR <- "data/CAP"
PATH_CAP_INDEX <- file.path(OUT_DIR, "cap_index.csv")
PATH_CAP_PAIRS <- file.path(OUT_DIR, "icefall_station_cap.csv")

CAP_ALGORITHM_VERSION <- "cap_dem_v1"

WINDOW_RADIUS_M <- 1400
INNER_RADIUS_M <- 250
LOCAL_RADIUS_M <- 650
OUTER_RING_MIN_M <- 900
GLACIER_RADIUS_M <- 7000

DEM_SPECS <- list(
  list(
    id = "tirol_5m",
    label = "DGM_Tirol_5m_epsg31254_2006_2020",
    path = "data/DEM/DGM_Tirol_5m_epsg31254_2006_2020.tif",
    dem_resolution_factor = 1.00
  ),
  list(
    id = "at_5m",
    label = "DGM_AT_5m_epsg31287",
    path = "data/DEM/DGM_AT_5m_epsg31287.tif",
    dem_resolution_factor = 0.95
  ),
  list(
    id = "eudem_25m",
    label = "eudem_dem_3035_europe",
    path = "data/DEM/eudem_dem_3035_europe.tif",
    dem_resolution_factor = 0.55
  )
)

args <- commandArgs(trailingOnly = TRUE)
force <- any(args == "--force") || identical(Sys.getenv("ICEFALL_FORCE_CAP", "0"), "1")
all_stations <- any(args == "--all-stations") || identical(Sys.getenv("CAP_ALL_STATIONS", "0"), "1")
uid_arg <- grep("^--uids=", args, value = TRUE)
uid_filter <- integer(0)
if (length(uid_arg) > 0) {
  uid_filter <- suppressWarnings(as.integer(trimws(unlist(strsplit(sub("^--uids=", "", uid_arg[[1]]), "[,;\\s]+")))))
  uid_filter <- sort(unique(uid_filter[is.finite(uid_filter)]))
}

dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

clamp01 <- function(x) pmax(0, pmin(1, x))

to_num <- function(x) {
  if (is.null(x)) return(NA_real_)
  if (is.numeric(x)) return(x)
  x <- as.character(x)
  x <- gsub(",", ".", x, fixed = TRUE)
  suppressWarnings(as.numeric(x))
}

safe_quantile <- function(x, prob) {
  x <- x[is.finite(x)]
  if (length(x) == 0) return(NA_real_)
  as.numeric(stats::quantile(x, probs = prob, na.rm = TRUE, type = 8))
}

safe_mean <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) return(NA_real_)
  mean(x)
}

first_existing_col <- function(df, candidates) {
  hit <- candidates[candidates %in% names(df)]
  if (length(hit) == 0) return(NA_character_)
  hit[[1]]
}

entity_key <- function(entity_type, entity_id) {
  paste(entity_type, as.character(entity_id), sep = ":")
}

cap_class <- function(x) {
  dplyr::case_when(
    !is.finite(x) ~ "missing",
    x >= 0.70 ~ "high",
    x >= 0.40 ~ "medium",
    TRUE ~ "low"
  )
}

empty_cap_row <- function(entity, qa_flag, qa_note = NA_character_) {
  tibble(
    entity_type = entity$entity_type,
    entity_id = as.character(entity$entity_id),
    name = entity$name,
    lat = entity$lat,
    lon = entity$lon,
    dem_source = NA_character_,
    dem_resolution_m = NA_real_,
    elev_dem_m = NA_real_,
    cap_potential = NA_real_,
    cap_class = "missing",
    cold_air_accumulation_score = NA_real_,
    valley_bottom_score = NA_real_,
    drainage_block_score = NA_real_,
    terrain_enclosure_score = NA_real_,
    glacier_source_score = NA_real_,
    low_slope_score = NA_real_,
    local_tpi_m = NA_real_,
    relief_q90_m = NA_real_,
    outer_spill_drop_m = NA_real_,
    lower_outer_fraction = NA_real_,
    higher_area_ha = NA_real_,
    glacier_weighted_area_ha = NA_real_,
    cap_confidence = 0,
    qa_flag = qa_flag,
    qa_note = qa_note,
    cap_algorithm_version = CAP_ALGORITHM_VERSION
  )
}

load_dem_catalog <- function() {
  lapply(DEM_SPECS, function(spec) {
    if (!file.exists(spec$path)) stop("Missing DEM: ", spec$path)
    r <- terra::rast(spec$path)
    spec$resolution_m <- mean(terra::res(r))
    list(spec = spec, raster = r)
  })
}

choose_dem_for_point <- function(point_ll, dem_catalog) {
  for (entry in dem_catalog) {
    pt_proj <- suppressWarnings(terra::project(point_ll, terra::crs(entry$raster)))
    z <- tryCatch(as.numeric(terra::extract(entry$raster, pt_proj)[1, 2]), error = function(e) NA_real_)
    if (is.finite(z)) {
      return(list(
        spec = entry$spec,
        raster = entry$raster,
        point = pt_proj,
        point_elev_m = z
      ))
    }
  }
  NULL
}

make_glacier_projector <- function(path) {
  if (!file.exists(path)) {
    message("CAP: glacier shapefile not found: ", path, " (glacier_source_score=0)")
    return(function(crs_value) NULL)
  }
  raw <- tryCatch(terra::vect(path), error = function(e) NULL)
  if (is.null(raw)) {
    message("CAP: could not read glacier shapefile: ", path, " (glacier_source_score=0)")
    return(function(crs_value) NULL)
  }
  cache <- new.env(parent = emptyenv())
  function(crs_value) {
    key <- paste0("crs_", abs(sum(utf8ToInt(as.character(crs_value)), na.rm = TRUE)))
    if (exists(key, envir = cache, inherits = FALSE)) return(get(key, envir = cache))
    out <- tryCatch(terra::project(raw, crs_value), error = function(e) NULL)
    assign(key, out, envir = cache)
    out
  }
}

compute_glacier_source <- function(dem_choice, z0, get_glaciers) {
  glaciers <- get_glaciers(terra::crs(dem_choice$raster))
  if (is.null(glaciers)) {
    return(list(score = 0, weighted_area_ha = 0))
  }

  buf <- terra::buffer(dem_choice$point, width = GLACIER_RADIUS_M)
  gl_crop <- tryCatch(terra::crop(glaciers, buf), error = function(e) NULL)
  if (is.null(gl_crop) || nrow(gl_crop) == 0) {
    return(list(score = 0, weighted_area_ha = 0))
  }

  gl_clip <- tryCatch(terra::intersect(gl_crop, buf), error = function(e) gl_crop)
  if (is.null(gl_clip) || nrow(gl_clip) == 0) {
    return(list(score = 0, weighted_area_ha = 0))
  }

  areas_km2 <- tryCatch(terra::expanse(gl_clip, unit = "km"), error = function(e) rep(NA_real_, nrow(gl_clip)))
  areas_km2 <- as.numeric(areas_km2)
  cents <- tryCatch(terra::centroids(gl_clip), error = function(e) NULL)
  if (is.null(cents) || nrow(cents) == 0) {
    return(list(score = 0, weighted_area_ha = 0))
  }

  cxy <- terra::crds(cents, df = TRUE)
  pxy <- terra::crds(dem_choice$point, df = TRUE)[1, c("x", "y")]
  dist_m <- sqrt((cxy$x - pxy$x)^2 + (cxy$y - pxy$y)^2)
  cz <- tryCatch(as.numeric(terra::extract(dem_choice$raster, cents)[, 2]), error = function(e) rep(NA_real_, nrow(cxy)))

  above_fac <- clamp01((cz - z0 - 50) / 600)
  above_fac[!is.finite(above_fac)] <- 0.5
  dist_fac <- exp(-dist_m / 3000)
  area_ha <- areas_km2 * 100
  area_ha[!is.finite(area_ha)] <- 0

  weighted_area_ha <- sum(area_ha * above_fac * dist_fac, na.rm = TRUE)
  score <- clamp01(log1p(weighted_area_ha) / log1p(120))
  list(score = score, weighted_area_ha = weighted_area_ha)
}

compute_cap_for_entity <- function(entity, dem_catalog, get_glaciers) {
  if (!is.finite(entity$lat) || !is.finite(entity$lon)) {
    return(empty_cap_row(entity, "missing_coordinates"))
  }

  point_ll <- terra::vect(
    data.frame(lon = entity$lon, lat = entity$lat),
    geom = c("lon", "lat"),
    crs = "EPSG:4326"
  )
  dem_choice <- choose_dem_for_point(point_ll, dem_catalog)
  if (is.null(dem_choice)) {
    return(empty_cap_row(entity, "no_dem_hit", "No DEM returned a valid elevation at the point."))
  }

  buf <- terra::buffer(dem_choice$point, width = WINDOW_RADIUS_M)
  dem_local <- tryCatch(terra::crop(dem_choice$raster, terra::ext(buf), snap = "out"), error = function(e) NULL)
  if (is.null(dem_local) || terra::ncell(dem_local) == 0) {
    return(empty_cap_row(entity, "empty_dem_window", "No DEM cells in the local CAP window."))
  }
  names(dem_local) <- "elev_m"

  xy0 <- terra::crds(dem_choice$point, df = TRUE)[1, c("x", "y")]
  df <- terra::as.data.frame(dem_local, xy = TRUE, na.rm = FALSE)
  if (!"elev_m" %in% names(df)) names(df)[ncol(df)] <- "elev_m"
  df$elev_m <- to_num(df$elev_m)
  df$dist_m <- sqrt((df$x - xy0$x)^2 + (df$y - xy0$y)^2)
  df <- df[is.finite(df$elev_m) & is.finite(df$dist_m) & df$dist_m <= WINDOW_RADIUS_M, , drop = FALSE]
  if (nrow(df) < 100) {
    return(empty_cap_row(entity, "insufficient_dem_cells", "Too few valid DEM cells in the local CAP window."))
  }

  z0 <- dem_choice$point_elev_m
  cell_area_m2 <- prod(terra::res(dem_local))
  vals <- df$elev_m
  inner_vals <- df$elev_m[df$dist_m <= INNER_RADIUS_M]
  local_vals <- df$elev_m[df$dist_m <= LOCAL_RADIUS_M]
  outer_vals <- df$elev_m[df$dist_m >= OUTER_RING_MIN_M & df$dist_m <= WINDOW_RADIUS_M]
  if (length(outer_vals) < 50) outer_vals <- df$elev_m[df$dist_m >= LOCAL_RADIUS_M]

  elev_rank <- mean(vals <= z0, na.rm = TRUE)
  local_tpi_m <- z0 - safe_mean(local_vals)
  relief_q90_m <- safe_quantile(vals, 0.90) - z0
  median_relief_m <- stats::median(vals, na.rm = TRUE) - z0

  slope_vals <- tryCatch({
    slope <- terra::terrain(dem_local, v = "slope", unit = "degrees", neighbors = 8)
    as.numeric(terra::extract(slope, dem_choice$point, buffer = INNER_RADIUS_M)[, 2])
  }, error = function(e) NA_real_)
  mean_inner_slope_deg <- safe_mean(slope_vals)
  low_slope_score <- clamp01(1 - (mean_inner_slope_deg - 7) / 28)
  if (!is.finite(low_slope_score)) low_slope_score <- 0.5

  valley_bottom_score <- clamp01(
    0.45 * clamp01((0.50 - elev_rank) / 0.40) +
      0.35 * clamp01(median_relief_m / 250) +
      0.20 * low_slope_score
  )

  higher_cells <- df$elev_m > (z0 + 40)
  higher_area_ha <- sum(higher_cells, na.rm = TRUE) * cell_area_m2 / 10000
  cold_air_accumulation_score <- clamp01(log1p(higher_area_ha) / log1p(350)) *
    clamp01(relief_q90_m / 700)

  outer_min_m <- min(outer_vals, na.rm = TRUE)
  outer_spill_drop_m <- z0 - outer_min_m
  lower_outer_fraction <- mean(outer_vals < (z0 - 20), na.rm = TRUE)
  drainage_block_score <- clamp01(1 - outer_spill_drop_m / 180) *
    clamp01(1 - lower_outer_fraction / 0.12)

  az_deg <- (atan2(df$x - xy0$x, df$y - xy0$y) * 180 / pi + 360) %% 360
  sector <- floor(az_deg / 45)
  sector_q90 <- tapply(df$elev_m - z0, sector, function(x) safe_quantile(x, 0.90))
  sector_score <- clamp01((as.numeric(sector_q90) - 60) / 400)
  terrain_enclosure_score <- mean(sector_score, na.rm = TRUE) * clamp01(length(sector_score) / 8)
  if (!is.finite(terrain_enclosure_score)) terrain_enclosure_score <- 0

  glacier <- compute_glacier_source(dem_choice, z0, get_glaciers)

  cap_potential <- clamp01(
    0.25 * cold_air_accumulation_score +
      0.25 * valley_bottom_score +
      0.20 * drainage_block_score +
      0.15 * terrain_enclosure_score +
      0.15 * glacier$score
  )

  cap_confidence <- clamp01(dem_choice$spec$dem_resolution_factor * min(1, nrow(df) / 1000))

  tibble(
    entity_type = entity$entity_type,
    entity_id = as.character(entity$entity_id),
    name = entity$name,
    lat = entity$lat,
    lon = entity$lon,
    dem_source = dem_choice$spec$label,
    dem_resolution_m = dem_choice$spec$resolution_m,
    elev_dem_m = z0,
    cap_potential = cap_potential,
    cap_class = cap_class(cap_potential),
    cold_air_accumulation_score = cold_air_accumulation_score,
    valley_bottom_score = valley_bottom_score,
    drainage_block_score = drainage_block_score,
    terrain_enclosure_score = terrain_enclosure_score,
    glacier_source_score = glacier$score,
    low_slope_score = low_slope_score,
    local_tpi_m = local_tpi_m,
    relief_q90_m = relief_q90_m,
    outer_spill_drop_m = outer_spill_drop_m,
    lower_outer_fraction = lower_outer_fraction,
    higher_area_ha = higher_area_ha,
    glacier_weighted_area_ha = glacier$weighted_area_ha,
    cap_confidence = cap_confidence,
    qa_flag = "ok",
    qa_note = NA_character_,
    cap_algorithm_version = CAP_ALGORITHM_VERSION
  )
}

stopifnot(file.exists(PATH_ASSIGN), file.exists(PATH_STATIONS))

assign <- readr::read_csv(PATH_ASSIGN, show_col_types = FALSE, progress = FALSE)
stations_all <- readr::read_csv(PATH_STATIONS, show_col_types = FALSE, progress = FALSE)

if (length(uid_filter) > 0) {
  assign <- assign %>% filter(uid %in% uid_filter)
  if (nrow(assign) == 0) stop("No assignment rows left after --uids filter.")
}

icefalls <- assign %>%
  transmute(
    entity_type = "icefall",
    entity_id = as.character(uid),
    name = if ("icefall_name" %in% names(.)) as.character(icefall_name) else as.character(uid),
    lat = to_num(ice_lat),
    lon = to_num(ice_lon)
  ) %>%
  distinct(entity_type, entity_id, .keep_all = TRUE)

station_ids <- if (all_stations) {
  unique(as.character(stations_all$station_id))
} else {
  unique(as.character(assign$station_id))
}

stations <- stations_all %>%
  filter(as.character(station_id) %in% station_ids) %>%
  transmute(
    entity_type = "station",
    entity_id = as.character(station_id),
    name = as.character(name),
    lat = to_num(lat),
    lon = to_num(lon)
  ) %>%
  distinct(entity_type, entity_id, .keep_all = TRUE)

entities <- bind_rows(icefalls, stations) %>%
  mutate(entity_key = entity_key(entity_type, entity_id)) %>%
  filter(is.finite(lat), is.finite(lon))

if (nrow(entities) == 0) stop("No valid CAP entities found.")

existing <- tibble()
if (file.exists(PATH_CAP_INDEX) && !force) {
  existing <- readr::read_csv(PATH_CAP_INDEX, show_col_types = FALSE, progress = FALSE) %>%
    mutate(entity_key = entity_key(entity_type, entity_id)) %>%
    filter(cap_algorithm_version == CAP_ALGORITHM_VERSION)
}

todo <- entities
if (nrow(existing) > 0) {
  todo <- entities %>% filter(!entity_key %in% existing$entity_key)
}

message("CAP entities total: ", nrow(entities), "; cached: ", nrow(entities) - nrow(todo), "; to compute: ", nrow(todo))

dem_catalog <- load_dem_catalog()
get_glaciers <- make_glacier_projector(PATH_GLACIERS)

new_rows <- vector("list", nrow(todo))
if (nrow(todo) > 0) {
  for (i in seq_len(nrow(todo))) {
    ent <- todo[i, ]
    message(sprintf("CAP %s %s (%d/%d)", ent$entity_type, ent$entity_id, i, nrow(todo)))
    new_rows[[i]] <- tryCatch(
      compute_cap_for_entity(ent, dem_catalog, get_glaciers),
      error = function(e) empty_cap_row(ent, "error", e$message)
    )
  }
}

cap_index <- bind_rows(
  existing %>% select(-any_of("entity_key")),
  bind_rows(new_rows)
) %>%
  arrange(entity_type, suppressWarnings(as.integer(entity_id)), entity_id)

readr::write_csv(cap_index, PATH_CAP_INDEX)

ice_cap <- cap_index %>%
  filter(entity_type == "icefall") %>%
  transmute(
    uid = suppressWarnings(as.integer(entity_id)),
    icefall_cap_potential = cap_potential,
    icefall_cap_class = cap_class,
    icefall_cap_confidence = cap_confidence
  )

station_cap <- cap_index %>%
  filter(entity_type == "station") %>%
  transmute(
    station_id = as.character(entity_id),
    station_cap_potential = cap_potential,
    station_cap_class = cap_class,
    station_cap_confidence = cap_confidence
  )

cap_pairs <- assign %>%
  transmute(
    uid = as.integer(uid),
    icefall_name = if ("icefall_name" %in% names(.)) as.character(icefall_name) else as.character(uid),
    station_id = as.character(station_id),
    station_name = if ("station_name" %in% names(.)) as.character(station_name) else NA_character_
  ) %>%
  left_join(ice_cap, by = "uid") %>%
  left_join(station_cap, by = "station_id") %>%
  mutate(
    cap_delta = icefall_cap_potential - station_cap_potential,
    cap_delta = if_else(is.finite(cap_delta), pmax(-1, pmin(1, cap_delta)), 0),
    cap_delta_positive = pmax(0, cap_delta),
    cap_pair_confidence = pmin(
      if_else(is.finite(icefall_cap_confidence), icefall_cap_confidence, 0),
      if_else(is.finite(station_cap_confidence), station_cap_confidence, 0)
    )
  ) %>%
  arrange(uid)

readr::write_csv(cap_pairs, PATH_CAP_PAIRS)

message("Wrote ", PATH_CAP_INDEX)
message("Wrote ", PATH_CAP_PAIRS)
