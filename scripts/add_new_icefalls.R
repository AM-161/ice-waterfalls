#!/usr/bin/env Rscript
# Add or update icefalls from add_new/new_icefalls.csv and rebuild the
# fixed per-icefall parameter tables used by the model and list page.

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(terra)
  library(geosphere)
  library(sf)
})

terra::terraOptions(progress = 0)
sf::sf_use_s2(FALSE)

# ---------------------------------------------------------------------
# Arguments and project root
# ---------------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)

has_arg <- function(flag) flag %in% args

arg_value <- function(prefix, default = NA_character_) {
  hit <- grep(paste0("^", prefix), args, value = TRUE)
  if (length(hit) == 0) return(default)
  sub(paste0("^", prefix), "", hit[[1]])
}

parse_uid_arg <- function(x) {
  if (is.na(x) || !nzchar(trimws(x))) return(integer(0))
  out <- suppressWarnings(as.integer(trimws(unlist(strsplit(x, "[,;\\s]+")))))
  sort(unique(out[is.finite(out)]))
}

script_path <- tryCatch(normalizePath(sys.frame(1)$ofile, winslash = "/", mustWork = TRUE), error = function(e) NA_character_)
script_dir <- if (!is.na(script_path)) dirname(script_path) else normalizePath(getwd(), winslash = "/", mustWork = FALSE)

find_project_root <- function(start_dir) {
  d <- normalizePath(start_dir, winslash = "/", mustWork = FALSE)
  for (i in 0:10) {
    if (file.exists(file.path(d, "ice-waterfalls.Rproj")) &&
        file.exists(file.path(d, "data", "Koordinaten_Wasserfaelle", "eisklettern_links_entries_diff.csv"))) {
      return(d)
    }
    parent <- normalizePath(file.path(d, ".."), winslash = "/", mustWork = FALSE)
    if (identical(parent, d)) break
    d <- parent
  }
  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

PROJECT_ROOT <- find_project_root(script_dir)
setwd(PROJECT_ROOT)

PATH_INPUT <- arg_value("--input=", "add_new/new_icefalls.csv")
PATH_META <- "data/Koordinaten_Wasserfaelle/eisklettern_links_entries_diff.csv"
PATH_ASSIGN <- "data/AWS/icefalls_nearest_station.csv"
PATH_STATIONS <- "data/AWS/stations_all.csv"
PATH_WIND <- "data/Wind/wind_vulnerability_5deg.csv"
DIR_SUN <- "data/suntime"

DRY_RUN <- has_arg("--dry-run")
SKIP_SUN <- has_arg("--skip-sun")
SKIP_WIND <- has_arg("--skip-wind")
SKIP_STATION <- has_arg("--skip-station")
SKIP_STRUCTURE <- has_arg("--skip-structure")
SKIP_CAP <- has_arg("--skip-cap")
SKIP_SITE <- has_arg("--skip-site")
RUN_MODELS <- has_arg("--run-models")
BUILD_MAP <- has_arg("--build-map")
KEEP_EXISTING_SUN <- has_arg("--keep-existing-sun")
FORCE_STRUCTURE <- has_arg("--force-structure")
FORCE_CAP <- has_arg("--force-cap")

uid_filter <- parse_uid_arg(arg_value("--uids=", ""))

today <- Sys.Date()
season_year <- if (lubridate::month(today) >= 10) lubridate::year(today) else lubridate::year(today) - 1L
SUN_START <- as.Date(arg_value("--sun-start=", sprintf("%d-11-01", season_year)))
SUN_END <- as.Date(arg_value("--sun-end=", sprintf("%d-04-30", season_year + 1L)))
SUN_STEP_MIN <- suppressWarnings(as.integer(arg_value("--sun-step-min=", "1")))
if (!is.finite(SUN_STEP_MIN) || SUN_STEP_MIN < 1L) SUN_STEP_MIN <- 1L

HORIZON_BUFFER_M <- suppressWarnings(as.numeric(arg_value("--horizon-buffer-m=", "6000")))
HORIZON_MAX_DIST_M <- suppressWarnings(as.numeric(arg_value("--horizon-max-dist-m=", "6000")))
HORIZON_STEP_M <- suppressWarnings(as.numeric(arg_value("--horizon-step-m=", "10")))
HORIZON_N_AZ <- suppressWarnings(as.integer(arg_value("--horizon-n-az=", "1440")))
if (!is.finite(HORIZON_BUFFER_M) || HORIZON_BUFFER_M <= 0) HORIZON_BUFFER_M <- 6000
if (!is.finite(HORIZON_MAX_DIST_M) || HORIZON_MAX_DIST_M <= 0) HORIZON_MAX_DIST_M <- 6000
if (!is.finite(HORIZON_STEP_M) || HORIZON_STEP_M <= 0) HORIZON_STEP_M <- 10
if (!is.finite(HORIZON_N_AZ) || HORIZON_N_AZ < 90L) HORIZON_N_AZ <- 1440L

msg <- function(...) message(paste0(...))

stop_if_missing <- function(path, label = path) {
  if (!file.exists(path)) stop("Missing ", label, ": ", path, call. = FALSE)
}

rscript_bin <- file.path(R.home("bin"), if (.Platform$OS.type == "windows") "Rscript.exe" else "Rscript")
if (!file.exists(rscript_bin)) stop("Could not locate Rscript via R.home(): ", rscript_bin, call. = FALSE)

run_rscript <- function(script, extra_args = character(0), env = character(0), required = TRUE) {
  if (!file.exists(script)) {
    if (required) stop("Missing script: ", script, call. = FALSE)
    msg("Skip missing script: ", script)
    return(invisible(FALSE))
  }
  cmd_args <- c(script, extra_args)
  msg("Run: ", shQuote(rscript_bin), " ", paste(shQuote(cmd_args), collapse = " "))
  status <- system2(rscript_bin, cmd_args, env = env)
  if (!identical(status, 0L)) {
    stop("Script failed (exit ", status, "): ", script, call. = FALSE)
  }
  invisible(TRUE)
}

# ---------------------------------------------------------------------
# Generic CSV helpers
# ---------------------------------------------------------------------

detect_delim <- function(path) {
  header <- readLines(path, n = 1, warn = FALSE)
  if (length(header) == 0) stop("Empty file: ", path, call. = FALSE)
  counts <- c(
    ";" = stringr::str_count(header, stringr::fixed(";")),
    "," = stringr::str_count(header, stringr::fixed(",")),
    "\t" = stringr::str_count(header, stringr::fixed("\t"))
  )
  names(counts)[which.max(counts)]
}

read_any_csv <- function(path, force_character = TRUE) {
  stop_if_missing(path)
  col_spec <- if (force_character) readr::cols(.default = readr::col_character()) else readr::cols()
  readr::read_delim(
    file = path,
    delim = detect_delim(path),
    col_types = col_spec,
    show_col_types = FALSE,
    progress = FALSE,
    comment = "#"
  )
}

empty_to_na <- function(x) {
  x <- trimws(as.character(x))
  x[x %in% c("", "NA", "NaN", "NULL", "null")] <- NA_character_
  x
}

nonempty <- function(x) {
  !is.na(empty_to_na(x))
}

to_num <- function(x) {
  if (is.null(x)) return(NA_real_)
  if (is.numeric(x)) return(x)
  x <- as.character(x)
  x[x %in% c("", "NA", "NaN", "NULL", "null")] <- NA_character_
  x <- gsub(",", ".", x, fixed = TRUE)
  suppressWarnings(as.numeric(x))
}

parse_uid <- function(x) {
  suppressWarnings(as.integer(readr::parse_number(as.character(x))))
}

clean_name <- function(x) {
  x <- trimws(tolower(as.character(x)))
  x_ascii <- suppressWarnings(iconv(x, from = "", to = "ASCII//TRANSLIT"))
  x[!is.na(x_ascii)] <- x_ascii[!is.na(x_ascii)]
  x <- gsub("\ufeff", "", x, fixed = TRUE)
  x <- gsub("[^a-z0-9_]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  x
}

canonical_meta_cols <- c(
  "uid", "name", "latitude", "longitude", "hoehe_dgm5m",
  "erstbegehnung", "schwierigkeit", "eisfallhhe",
  "zustieg", "abstieg", "beschreibung",
  "ausrichtung", "topo_url", "url", "himmelsrichtung"
)

standardize_new_icefall_cols <- function(df) {
  names(df) <- clean_name(names(df))
  alias <- list(
    uid = c("uid", "id"),
    name = c("name", "eisfall", "icefall", "icefall_name", "icefall_name"),
    latitude = c("latitude", "lat", "breitengrad", "y"),
    longitude = c("longitude", "lon", "lng", "laengengrad", "langengrad", "x"),
    hoehe_dgm5m = c("hoehe_dgm5m", "hoehe", "hohe", "elev_m", "elevation", "altitude_m"),
    erstbegehnung = c("erstbegehnung", "first_ascent"),
    schwierigkeit = c("schwierigkeit", "difficulty", "grad", "grade"),
    eisfallhhe = c("eisfallhhe", "eisfallhoehe", "eisfallhohe", "eisfallh_he", "height_m", "icefall_height_m", "height"),
    zustieg = c("zustieg", "approach"),
    abstieg = c("abstieg", "descent"),
    beschreibung = c("beschreibung", "description", "notes", "notiz", "notizen"),
    ausrichtung = c("ausrichtung", "aspect", "aspect_deg", "exposition_deg"),
    topo_url = c("topo_url", "topo", "topolink", "topo_link"),
    url = c("url", "source_url", "quelle"),
    himmelsrichtung = c("himmelsrichtung", "aspect_cardinal", "exposition", "richtung")
  )

  for (target in names(alias)) {
    if (target %in% names(df)) next
    hit <- alias[[target]][alias[[target]] %in% names(df)]
    if (length(hit) > 0) names(df)[match(hit[[1]], names(df))] <- target
  }

  df
}

drop_blank_rows <- function(df) {
  if (nrow(df) == 0) return(df)
  keep <- apply(as.data.frame(df), 1, function(row) any(nonempty(row)))
  df[keep, , drop = FALSE]
}

read_meta <- function(path) {
  meta <- read_any_csv(path, force_character = TRUE)
  names(meta) <- clean_name(names(meta))
  for (nm in canonical_meta_cols) {
    if (!nm %in% names(meta)) meta[[nm]] <- NA_character_
  }
  extra <- setdiff(names(meta), canonical_meta_cols)
  meta <- meta[, c(canonical_meta_cols, extra), drop = FALSE]
  meta
}

write_semicolon_csv <- function(df, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  readr::write_delim(df, path, delim = ";", na = "")
}

format_uid_file <- function(uid) {
  uid <- as.integer(uid)
  if (is.finite(uid) && uid >= 0 && uid < 1000) return(sprintf("%03d", uid))
  as.character(uid)
}

find_next_uid <- function(used) {
  next_uid <- max(c(used, 0), na.rm = TRUE) + 1L
  while (next_uid %in% used) next_uid <- next_uid + 1L
  next_uid
}

# ---------------------------------------------------------------------
# DEM helpers
# ---------------------------------------------------------------------

DEM_SPECS <- list(
  list(id = "tirol_5m", label = "DGM_Tirol_5m_epsg31254_2006_2020", path = "data/DEM/DGM_Tirol_5m_epsg31254_2006_2020.tif", factor = 1.00),
  list(id = "at_5m", label = "DGM_AT_5m_epsg31287", path = "data/DEM/DGM_AT_5m_epsg31287.tif", factor = 0.95),
  list(id = "eudem_25m", label = "eudem_dem_3035_europe", path = "data/DEM/eudem_dem_3035_europe.tif", factor = 0.55)
)

load_dem_catalog <- function(required = FALSE) {
  out <- list()
  for (spec in DEM_SPECS) {
    if (!file.exists(spec$path)) {
      msg("DEM not found, skip: ", spec$path)
      next
    }
    r <- terra::rast(spec$path)
    spec$resolution_m <- mean(terra::res(r))
    out[[length(out) + 1L]] <- list(spec = spec, raster = r)
  }
  if (required && length(out) == 0) stop("No DEM files found.", call. = FALSE)
  out
}

point_ll <- function(lon, lat) {
  terra::vect(data.frame(lon = lon, lat = lat), geom = c("lon", "lat"), crs = "EPSG:4326")
}

extract_point <- function(r, pt, buffer = NA_real_, fun = mean) {
  out <- tryCatch({
    if (is.finite(buffer) && buffer > 0) {
      as.numeric(terra::extract(r, pt, buffer = buffer, fun = fun, na.rm = TRUE)[1, 2])
    } else {
      as.numeric(terra::extract(r, pt)[1, 2])
    }
  }, error = function(e) NA_real_)
  if (length(out) == 0 || !is.finite(out)) NA_real_ else out
}

choose_dem_for_lonlat <- function(lon, lat, dem_catalog) {
  if (!is.finite(lon) || !is.finite(lat)) return(NULL)
  p_ll <- point_ll(lon, lat)
  for (entry in dem_catalog) {
    pt <- tryCatch(terra::project(p_ll, terra::crs(entry$raster)), error = function(e) NULL)
    if (is.null(pt)) next
    z <- extract_point(entry$raster, pt)
    if (is.finite(z)) {
      return(list(spec = entry$spec, raster = entry$raster, point = pt, point_elev_m = z))
    }
  }
  NULL
}

aspect_cache_env <- new.env(parent = emptyenv())

aspect_cache_path <- function(spec) {
  file.path("data", "DEM", "_cache_aspect", paste0(spec$id, "_aspect_deg_cache.tif"))
}

get_aspect_raster <- function(entry) {
  key <- entry$spec$id
  if (exists(key, envir = aspect_cache_env, inherits = FALSE)) return(get(key, envir = aspect_cache_env))
  path <- aspect_cache_path(entry$spec)
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  if (file.exists(path)) {
    asp <- terra::rast(path)
  } else {
    msg("Build aspect cache: ", path)
    fact <- if (entry$spec$resolution_m <= 10) 2L else 1L
    r <- if (fact > 1L) terra::aggregate(entry$raster, fact = fact, fun = mean, na.rm = TRUE) else entry$raster
    asp <- terra::terrain(r, v = "aspect", unit = "degrees")
    terra::writeRaster(asp, path, overwrite = TRUE)
  }
  assign(key, asp, envir = aspect_cache_env)
  asp
}

deg_to_dir8 <- function(deg) {
  out <- rep("", length(deg))
  ok <- is.finite(deg)
  if (!any(ok)) return(out)
  labels <- c("N", "NO", "O", "SO", "S", "SW", "W", "NW")
  idx <- floor(((deg[ok] %% 360) + 22.5) / 45) %% 8
  out[ok] <- labels[idx + 1L]
  out
}

update_height_aspect <- function(meta, affected_uids, dem_catalog) {
  msg("Update height/aspect for UIDs: ", paste(affected_uids, collapse = ", "))
  uid_num <- parse_uid(meta$uid)
  for (uid in affected_uids) {
    i <- which(uid_num == uid)
    if (length(i) == 0) next
    i <- i[[1]]
    lat <- to_num(meta$latitude[i])
    lon <- to_num(meta$longitude[i])
    choice <- choose_dem_for_lonlat(lon, lat, dem_catalog)
    if (is.null(choice)) {
      msg("  UID ", uid, ": no DEM hit; keep manual height/aspect if present.")
      next
    }
    elev <- extract_point(choice$raster, choice$point, buffer = 15, fun = mean)
    asp <- NA_real_
    asp_r <- tryCatch(get_aspect_raster(choice), error = function(e) NULL)
    if (!is.null(asp_r)) {
      pt_asp <- tryCatch(terra::project(point_ll(lon, lat), terra::crs(asp_r)), error = function(e) NULL)
      if (!is.null(pt_asp)) {
        asp_round <- round(asp_r)
        asp <- extract_point(asp_round, pt_asp, buffer = 15, fun = terra::modal)
      }
    }
    if (is.finite(elev)) meta$hoehe_dgm5m[i] <- format(elev, scientific = FALSE, trim = TRUE)
    if (is.finite(asp)) {
      asp_i <- as.integer(round(asp)) %% 360L
      meta$ausrichtung[i] <- as.character(asp_i)
      meta$himmelsrichtung[i] <- deg_to_dir8(asp_i)
    }
    msg("  UID ", uid, ": DEM=", choice$spec$id, ", elev=", ifelse(is.finite(elev), round(elev, 1), "NA"), ", aspect=", ifelse(is.finite(asp), round(asp), "NA"))
  }
  meta
}

# ---------------------------------------------------------------------
# Station assignment
# ---------------------------------------------------------------------

to_lgl <- function(x) {
  if (is.logical(x)) return(x)
  x <- trimws(tolower(as.character(x)))
  out <- ifelse(x %in% c("true", "t", "1", "yes", "y"), TRUE,
                ifelse(x %in% c("false", "f", "0", "no", "n", ""), FALSE, NA))
  out[is.na(out)] <- FALSE
  out
}

col_lgl <- function(df, col) {
  if (!col %in% names(df)) return(rep(FALSE, nrow(df)))
  to_lgl(df[[col]])
}

local_topo_position <- function(dem, pt, radius_m = 750) {
  z0 <- extract_point(dem, pt)
  if (!is.finite(z0)) return(NA_real_)
  buf <- terra::buffer(pt, width = radius_m)
  vals <- tryCatch(terra::values(terra::mask(terra::crop(dem, buf), buf), mat = FALSE, na.rm = TRUE), error = function(e) numeric(0))
  vals <- vals[is.finite(vals)]
  if (length(vals) < 10) return(NA_real_)
  mean(vals <= z0)
}

path_barrier_m <- function(dem, pt_a, pt_b, n = 80L) {
  xy_a <- terra::crds(pt_a, df = TRUE)[1, c("x", "y")]
  xy_b <- terra::crds(pt_b, df = TRUE)[1, c("x", "y")]
  smp <- terra::vect(
    data.frame(
      x = seq(xy_a$x, xy_b$x, length.out = n),
      y = seq(xy_a$y, xy_b$y, length.out = n)
    ),
    geom = c("x", "y"),
    crs = terra::crs(dem)
  )
  prof <- tryCatch(as.numeric(terra::extract(dem, smp)[, 2]), error = function(e) numeric(0))
  prof <- prof[is.finite(prof)]
  if (length(prof) < 5) return(NA_real_)
  z_a <- extract_point(dem, pt_a)
  z_b <- extract_point(dem, pt_b)
  if (!is.finite(z_a) || !is.finite(z_b)) return(NA_real_)
  max(0, max(prof, na.rm = TRUE) - max(z_a, z_b))
}

same_valley_dem_flag <- function(topo_pos_diff, barrier_m) {
  is.finite(topo_pos_diff) & is.finite(barrier_m) & topo_pos_diff <= 0.30 & barrier_m <= 120
}

compute_station_assignments <- function(meta, affected_uids) {
  if (!file.exists(PATH_STATIONS)) {
    msg("Station table missing; skip station assignment: ", PATH_STATIONS)
    return(tibble())
  }
  stations_raw <- readr::read_csv(PATH_STATIONS, show_col_types = FALSE, progress = FALSE)
  names(stations_raw) <- clean_name(names(stations_raw))

  station_params <- stations_raw %>%
    transmute(
      station_id = as.character(station_id),
      has_temp = col_lgl(., "tl"),
      has_precip = col_lgl(., "rr"),
      has_rh = col_lgl(., "rf"),
      has_td = col_lgl(., "td_lwd"),
      has_rh_or_td = has_rh | has_td
    ) %>%
    distinct(station_id, .keep_all = TRUE)

  stations <- stations_raw %>%
    mutate(
      station_lon = to_num(lon),
      station_lat = to_num(lat),
      station_altitude_m = to_num(altitude_m)
    ) %>%
    filter(is.finite(station_lon), is.finite(station_lat)) %>%
    transmute(
      station_id = as.character(station_id),
      station_name = as.character(name),
      source = as.character(source),
      station_lat, station_lon, station_altitude_m
    ) %>%
    left_join(station_params, by = "station_id")

  if (nrow(stations) == 0) stop("No stations with valid coordinates found.", call. = FALSE)

  ice <- meta %>%
    mutate(uid_num = parse_uid(uid), ice_lat = to_num(latitude), ice_lon = to_num(longitude), icefall_elev_meta = to_num(hoehe_dgm5m)) %>%
    filter(uid_num %in% affected_uids, is.finite(ice_lat), is.finite(ice_lon)) %>%
    transmute(uid = uid_num, icefall_name = as.character(name), ice_lat, ice_lon, icefall_elev_meta)

  if (nrow(ice) == 0) return(tibble())

  D <- geosphere::distm(
    as.matrix(ice[, c("ice_lon", "ice_lat")]),
    as.matrix(stations[, c("station_lon", "station_lat")]),
    fun = geosphere::distHaversine
  )

  dem <- NULL
  dem_topo <- NULL
  if (file.exists("data/DEM/DGM_Tirol_5m_epsg31254_2006_2020.tif")) {
    dem <- terra::rast("data/DEM/DGM_Tirol_5m_epsg31254_2006_2020.tif")
    fact <- max(1, round(25 / mean(terra::res(dem))))
    dem_topo <- if (fact > 1) terra::aggregate(dem, fact = fact, fun = mean, na.rm = TRUE) else dem
  }

  out_rows <- vector("list", nrow(ice))
  for (i in seq_len(nrow(ice))) {
    cand_idx <- order(D[i, ])[seq_len(min(12L, nrow(stations)))]
    dist_km <- as.numeric(D[i, cand_idx]) / 1000
    keep <- dist_km <= 20
    if (!any(keep) && length(keep) > 0) keep[1] <- TRUE
    cand_idx <- cand_idx[keep]
    dist_km <- dist_km[keep]
    cand <- stations[cand_idx, ] %>% mutate(dist_km = dist_km)

    ice_elev <- ice$icefall_elev_meta[i]
    ice_topo <- NA_real_
    station_topo <- rep(NA_real_, length(cand_idx))
    barrier <- rep(NA_real_, length(cand_idx))

    if (!is.null(dem) && !is.null(dem_topo)) {
      pt_ice <- tryCatch(terra::project(point_ll(ice$ice_lon[i], ice$ice_lat[i]), terra::crs(dem)), error = function(e) NULL)
      pt_ice_topo <- tryCatch(terra::project(point_ll(ice$ice_lon[i], ice$ice_lat[i]), terra::crs(dem_topo)), error = function(e) NULL)
      if (!is.null(pt_ice)) {
        z_dem <- extract_point(dem, pt_ice)
        if (is.finite(z_dem)) ice_elev <- z_dem
      }
      if (!is.null(pt_ice_topo)) {
        ice_topo <- local_topo_position(dem_topo, pt_ice_topo)
        for (j in seq_along(cand_idx)) {
          pt_st_topo <- tryCatch(terra::project(point_ll(cand$station_lon[j], cand$station_lat[j]), terra::crs(dem_topo)), error = function(e) NULL)
          if (!is.null(pt_st_topo)) {
            station_topo[j] <- local_topo_position(dem_topo, pt_st_topo)
            barrier[j] <- path_barrier_m(dem_topo, pt_ice_topo, pt_st_topo)
          }
        }
      }
    }

    elev_diff <- ice_elev - cand$station_altitude_m
    topo_diff <- abs(station_topo - ice_topo)
    param_penalty <- ifelse(cand$has_temp, 0, 100) + ifelse(cand$has_rh_or_td, 0, 50)
    thermal_score <- 1.0 * cand$dist_km +
      (1 / 400) * abs(elev_diff) +
      3.0 * ifelse(is.finite(topo_diff), topo_diff, 0.5) +
      (1 / 250) * pmax(0, ifelse(is.finite(barrier), barrier, 150) - 80) +
      param_penalty

    best <- which.min(thermal_score)

    out_rows[[i]] <- tibble(
      uid = ice$uid[i],
      icefall_name = ice$icefall_name[i],
      ice_lat = ice$ice_lat[i],
      ice_lon = ice$ice_lon[i],
      station_id = cand$station_id[best],
      station_name = cand$station_name[best],
      source = cand$source[best],
      station_lat = cand$station_lat[best],
      station_lon = cand$station_lon[best],
      dist_km = cand$dist_km[best],
      icefall_elev_m = ice_elev,
      elev_diff_m = elev_diff[best],
      has_temp = cand$has_temp[best],
      has_precip = cand$has_precip[best],
      has_rh_or_td = cand$has_rh_or_td[best],
      ice_topo_pos = ice_topo,
      station_topo_pos = station_topo[best],
      topo_pos_diff = topo_diff[best],
      path_barrier_m = barrier[best],
      same_valley_dem = same_valley_dem_flag(topo_diff[best], barrier[best]),
      thermal_score = thermal_score[best]
    )
  }

  bind_rows(out_rows)
}

upsert_csv_by_uid <- function(path, rows, uid_col = "uid", arrange_cols = uid_col) {
  if (is.null(rows) || nrow(rows) == 0) return(invisible(FALSE))
  old <- if (file.exists(path)) readr::read_csv(path, show_col_types = FALSE, progress = FALSE) else tibble()
  old_uid <- if (uid_col %in% names(old)) parse_uid(old[[uid_col]]) else integer(0)
  new_uid <- parse_uid(rows[[uid_col]])
  keep <- if (nrow(old) > 0) !(old_uid %in% new_uid) else logical(0)
  out <- bind_rows(old[keep, , drop = FALSE], rows)
  for (nm in setdiff(names(old), names(out))) out[[nm]] <- NA
  for (nm in setdiff(names(rows), names(out))) out[[nm]] <- NA
  if (all(arrange_cols %in% names(out))) out <- out %>% arrange(across(all_of(arrange_cols)))
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  readr::write_csv(out, path, na = "")
  invisible(TRUE)
}

# ---------------------------------------------------------------------
# Wind vulnerability
# ---------------------------------------------------------------------

compute_wind_lut <- function(meta, affected_uids, dem_catalog) {
  dirs <- seq(0, 355, by = 5)
  dists <- seq(50, 3000, by = 50)

  angle_to_vuln <- function(shelter_deg, cap_deg = 20) {
    shelter_deg <- pmax(0, shelter_deg)
    v <- 9 * (1 - pmin(shelter_deg, cap_deg) / cap_deg)
    as.integer(round(pmax(0, pmin(9, v))))
  }

  rows <- vector("list", length(affected_uids))
  uid_num <- parse_uid(meta$uid)
  for (k in seq_along(affected_uids)) {
    uid <- affected_uids[k]
    i <- which(uid_num == uid)
    if (length(i) == 0) next
    i <- i[[1]]
    lat <- to_num(meta$latitude[i])
    lon <- to_num(meta$longitude[i])
    choice <- choose_dem_for_lonlat(lon, lat, dem_catalog)
    if (is.null(choice)) {
      msg("  UID ", uid, ": no DEM for wind; write exposed fallback.")
      rows[[k]] <- tibble(uid = uid, dir_deg = dirs, wind_vuln_0_9 = 9L)
      next
    }
    z0 <- extract_point(choice$raster, choice$point)
    if (!is.finite(z0)) {
      rows[[k]] <- tibble(uid = uid, dir_deg = dirs, wind_vuln_0_9 = 9L)
      next
    }

    xy0 <- terra::crds(choice$point)[1, ]
    rad <- dirs * pi / 180
    x <- as.vector(sapply(rad, function(a) xy0[1] + dists * sin(a)))
    y <- as.vector(sapply(rad, function(a) xy0[2] + dists * cos(a)))
    dist_rep <- rep(dists, times = length(dirs))
    dir_rep <- rep(dirs, each = length(dists))
    pts <- terra::vect(data.frame(x = x, y = y), geom = c("x", "y"), crs = terra::crs(choice$raster))
    z <- tryCatch(as.numeric(terra::extract(choice$raster, pts)[, 2]), error = function(e) rep(NA_real_, length(x)))
    ang <- atan2((z - z0), dist_rep) * 180 / pi
    shelter <- tapply(ang, dir_rep, function(a) {
      a <- a[is.finite(a)]
      if (length(a) == 0) return(0)
      max(pmax(a, 0), na.rm = TRUE)
    })
    shelter_vec <- as.numeric(shelter[as.character(dirs)])
    shelter_vec[!is.finite(shelter_vec)] <- 0
    rows[[k]] <- tibble(uid = uid, dir_deg = dirs, wind_vuln_0_9 = angle_to_vuln(shelter_vec))
    msg("  UID ", uid, ": wind DEM=", choice$spec$id, ", mean vuln=", round(mean(rows[[k]]$wind_vuln_0_9), 2))
  }
  bind_rows(rows)
}

# ---------------------------------------------------------------------
# Topographic sun
# ---------------------------------------------------------------------

deg2rad <- function(x) x * pi / 180
rad2deg <- function(x) x * 180 / pi

tz_offset_hours_vec <- function(time_local) {
  z <- format(time_local, "%z")
  sign <- ifelse(substr(z, 1, 1) == "-", -1, 1)
  hh <- suppressWarnings(as.numeric(substr(z, 2, 3)))
  mm <- suppressWarnings(as.numeric(substr(z, 4, 5)))
  off <- sign * (hh + mm / 60)
  off[!is.finite(off)] <- 0
  off
}

compute_solar_position <- function(time_local, lat_deg, lon_deg) {
  n <- length(time_local)
  if (n == 0 || !is.finite(lat_deg) || !is.finite(lon_deg)) {
    return(list(altitude_deg = rep(NA_real_, n), azimuth_deg = rep(NA_real_, n)))
  }
  doy <- lubridate::yday(time_local)
  hour_local <- lubridate::hour(time_local) + lubridate::minute(time_local) / 60 + lubridate::second(time_local) / 3600
  gamma <- 2 * pi / 365.2422 * (doy - 1 + (hour_local - 12) / 24)
  eqtime_min <- 229.18 * (
    0.000075 +
      0.001868 * cos(gamma) -
      0.032077 * sin(gamma) -
      0.014615 * cos(2 * gamma) -
      0.040849 * sin(2 * gamma)
  )
  decl <- 0.006918 -
    0.399912 * cos(gamma) +
    0.070257 * sin(gamma) -
    0.006758 * cos(2 * gamma) +
    0.000907 * sin(2 * gamma) -
    0.002697 * cos(3 * gamma) +
    0.00148 * sin(3 * gamma)
  tz_hours <- tz_offset_hours_vec(time_local)
  true_solar_min <- (hour_local * 60 + eqtime_min + 4 * lon_deg - 60 * tz_hours) %% 1440
  hour_angle_deg <- true_solar_min / 4 - 180
  hour_angle_deg[hour_angle_deg < -180] <- hour_angle_deg[hour_angle_deg < -180] + 360
  lat_rad <- deg2rad(lat_deg)
  ha_rad <- deg2rad(hour_angle_deg)
  cos_zen <- sin(lat_rad) * sin(decl) + cos(lat_rad) * cos(decl) * cos(ha_rad)
  cos_zen <- pmin(1, pmax(-1, cos_zen))
  altitude_rad <- asin(cos_zen)
  azimuth_rad <- atan2(
    sin(ha_rad),
    cos(ha_rad) * sin(lat_rad) - tan(decl) * cos(lat_rad)
  )
  list(
    altitude_deg = rad2deg(altitude_rad),
    azimuth_deg = (rad2deg(azimuth_rad) + 180) %% 360
  )
}

compute_horizon_for_point <- function(choice, buffer_m, max_dist_m, step_m, n_az) {
  xy0 <- terra::crds(choice$point)[1, ]
  ext <- terra::ext(xy0[1] - buffer_m, xy0[1] + buffer_m, xy0[2] - buffer_m, xy0[2] + buffer_m)
  dem_local <- tryCatch(terra::crop(choice$raster, ext), error = function(e) NULL)
  if (is.null(dem_local) || terra::ncell(dem_local) == 0) stop("Empty DEM crop for horizon.", call. = FALSE)
  z0 <- extract_point(dem_local, choice$point)
  if (!is.finite(z0)) stop("No DEM elevation at point for horizon.", call. = FALSE)

  dirs <- seq(0, 360, length.out = n_az + 1L)[-(n_az + 1L)]
  dists <- seq(step_m, max_dist_m, by = step_m)
  rad <- dirs * pi / 180
  x <- as.vector(sapply(rad, function(a) xy0[1] + dists * sin(a)))
  y <- as.vector(sapply(rad, function(a) xy0[2] + dists * cos(a)))
  dist_rep <- rep(dists, times = length(dirs))
  dir_rep <- rep(dirs, each = length(dists))
  pts <- terra::vect(data.frame(x = x, y = y), geom = c("x", "y"), crs = terra::crs(dem_local))
  z <- tryCatch(as.numeric(terra::extract(dem_local, pts)[, 2]), error = function(e) rep(NA_real_, length(x)))
  ang <- atan2((z - z0), dist_rep) * 180 / pi
  horizon <- tapply(ang, dir_rep, function(a) {
    a <- a[is.finite(a)]
    if (length(a) == 0) return(0)
    max(pmax(a, 0), na.rm = TRUE)
  })
  h <- as.numeric(horizon[as.character(dirs)])
  h[!is.finite(h)] <- 0
  tibble(azimuth_deg = dirs, horizon_deg = h)
}

compute_sun_table <- function(uid, name, lat, lon, dem_catalog) {
  choice <- choose_dem_for_lonlat(lon, lat, dem_catalog)
  if (is.null(choice)) stop("No DEM hit for sun horizon UID ", uid, call. = FALSE)
  msg("  UID ", uid, ": sun DEM=", choice$spec$id)
  horizon <- compute_horizon_for_point(
    choice,
    buffer_m = HORIZON_BUFFER_M,
    max_dist_m = HORIZON_MAX_DIST_M,
    step_m = HORIZON_STEP_M,
    n_az = HORIZON_N_AZ
  )
  h_x <- c(horizon$azimuth_deg, 360)
  h_y <- c(horizon$horizon_deg, horizon$horizon_deg[[1]])
  dates <- seq.Date(SUN_START, SUN_END, by = "day")
  out <- vector("list", length(dates))
  for (i in seq_along(dates)) {
    day <- dates[[i]]
    t0 <- as.POSIXct(day, tz = "Europe/Vienna")
    t1 <- as.POSIXct(day + 1, tz = "Europe/Vienna") - SUN_STEP_MIN * 60
    tt <- seq(t0, t1, by = paste(SUN_STEP_MIN, "min"))
    sol <- compute_solar_position(tt, lat, lon)
    h_at_sun <- approx(h_x, h_y, xout = sol$azimuth_deg, rule = 2)$y
    sun_on <- is.finite(sol$altitude_deg) & sol$altitude_deg > h_at_sun
    sunrise <- if (any(sun_on)) min(tt[sun_on]) else as.POSIXct(NA, tz = "Europe/Vienna")
    sunset <- if (any(sun_on)) max(tt[sun_on]) else as.POSIXct(NA, tz = "Europe/Vienna")
    out[[i]] <- tibble(
      uid = uid,
      name = name,
      date = day,
      sunrise_topo = sunrise,
      sunset_topo = sunset,
      sun_hours_topo = sum(sun_on) * SUN_STEP_MIN / 60
    )
  }
  bind_rows(out)
}

write_sun_tables <- function(meta, affected_uids, dem_catalog) {
  dir.create(DIR_SUN, recursive = TRUE, showWarnings = FALSE)
  uid_num <- parse_uid(meta$uid)
  for (uid in affected_uids) {
    i <- which(uid_num == uid)
    if (length(i) == 0) next
    i <- i[[1]]
    lat <- to_num(meta$latitude[i])
    lon <- to_num(meta$longitude[i])
    name <- as.character(meta$name[i])
    if (!is.finite(lat) || !is.finite(lon)) {
      msg("  UID ", uid, ": skip sun, missing coordinates.")
      next
    }
    uid_fmt <- format_uid_file(uid)
    path <- file.path(DIR_SUN, paste0("sun_uid_", uid_fmt, ".csv"))
    if (file.exists(path) && KEEP_EXISTING_SUN) {
      msg("  UID ", uid, ": sun file exists; keep existing.")
      next
    }
    if (!KEEP_EXISTING_SUN) {
      old <- list.files(DIR_SUN, pattern = paste0("^sun_uid_", uid_fmt, ".*[.]csv$"), full.names = TRUE)
      if (length(old) > 0) unlink(old)
    }
    tbl <- tryCatch(
      compute_sun_table(uid, name, lat, lon, dem_catalog),
      error = function(e) {
        msg("  UID ", uid, ": sun failed: ", conditionMessage(e))
        NULL
      }
    )
    if (!is.null(tbl)) {
      readr::write_csv(tbl, path, na = "")
      msg("  wrote ", path)
    }
  }
}

# ---------------------------------------------------------------------
# Structure and CAP upserts around existing scripts
# ---------------------------------------------------------------------

read_csv_if_exists <- function(path) {
  if (file.exists(path)) readr::read_csv(path, show_col_types = FALSE, progress = FALSE) else tibble()
}

write_csv_safe <- function(df, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  readr::write_csv(df, path, na = "")
}

merge_uid_table <- function(old, new, affected_uids) {
  if (!"uid" %in% names(old)) old <- tibble()
  if (!"uid" %in% names(new)) new <- tibble()
  out <- bind_rows(
    old %>% filter(!(parse_uid(uid) %in% affected_uids)),
    new
  )
  if ("uid" %in% names(out)) out <- out %>% arrange(parse_uid(uid))
  out
}

merge_routes <- function(old_path, new_path, affected_uids) {
  old <- if (file.exists(old_path)) suppressWarnings(sf::st_read(old_path, quiet = TRUE)) else NULL
  new <- if (file.exists(new_path)) suppressWarnings(sf::st_read(new_path, quiet = TRUE)) else NULL
  parts <- list()
  if (!is.null(old) && nrow(old) > 0 && "uid" %in% names(old)) {
    parts[[length(parts) + 1L]] <- old[!(parse_uid(old$uid) %in% affected_uids), ]
  }
  if (!is.null(new) && nrow(new) > 0) {
    parts[[length(parts) + 1L]] <- new
  }
  if (length(parts) == 0) {
    if (file.exists(old_path)) unlink(old_path)
    return(invisible(FALSE))
  }
  out <- do.call(rbind, parts)
  if ("uid" %in% names(out)) out <- out[order(parse_uid(out$uid)), ]
  suppressWarnings(sf::st_write(out, old_path, quiet = TRUE, delete_dsn = TRUE))
  invisible(TRUE)
}

run_structure_update <- function(affected_uids) {
  script <- "analysis/icefall_structure/03_build_icefall_structure.R"
  out_dir <- "data/derived/icefall_structure"
  path_analysis <- file.path(out_dir, "icefall_structure_analysis.csv")
  path_qa <- file.path(out_dir, "icefall_structure_qa.csv")
  path_routes <- file.path(out_dir, "icefall_routes.geojson")

  old_analysis <- read_csv_if_exists(path_analysis)
  old_qa <- read_csv_if_exists(path_qa)
  old_routes_tmp <- tempfile(fileext = ".geojson")
  if (file.exists(path_routes)) file.copy(path_routes, old_routes_tmp, overwrite = TRUE)

  extra <- c(paste0("--uids=", paste(affected_uids, collapse = ",")))
  if (FORCE_STRUCTURE) extra <- c(extra, "--force")
  run_rscript(script, extra)

  new_analysis <- read_csv_if_exists(path_analysis)
  new_qa <- read_csv_if_exists(path_qa)
  new_routes_tmp <- tempfile(fileext = ".geojson")
  if (file.exists(path_routes)) file.copy(path_routes, new_routes_tmp, overwrite = TRUE)

  write_csv_safe(merge_uid_table(old_analysis, new_analysis, affected_uids), path_analysis)
  write_csv_safe(merge_uid_table(old_qa, new_qa, affected_uids), path_qa)

  if (file.exists(old_routes_tmp) || file.exists(new_routes_tmp)) {
    if (file.exists(old_routes_tmp)) file.copy(old_routes_tmp, path_routes, overwrite = TRUE) else if (file.exists(path_routes)) unlink(path_routes)
    merge_routes(path_routes, new_routes_tmp, affected_uids)
  }
}

merge_cap_index <- function(old, new) {
  key <- function(df) paste(as.character(df$entity_type), as.character(df$entity_id), sep = ":")
  if (!all(c("entity_type", "entity_id") %in% names(old))) old <- tibble()
  if (!all(c("entity_type", "entity_id") %in% names(new))) new <- tibble()
  old_key <- if (nrow(old) > 0) key(old) else character(0)
  new_key <- if (nrow(new) > 0) key(new) else character(0)
  out <- bind_rows(old[!(old_key %in% new_key), , drop = FALSE], new)
  if (all(c("entity_type", "entity_id") %in% names(out))) {
    out <- out %>% arrange(entity_type, suppressWarnings(as.integer(entity_id)), entity_id)
  }
  out
}

run_cap_update <- function(affected_uids) {
  script <- "analysis/cold_air_pooling/05_build_cap_index.R"
  path_index <- "data/CAP/cap_index.csv"
  path_pairs <- "data/CAP/icefall_station_cap.csv"
  old_index <- read_csv_if_exists(path_index)
  old_pairs <- read_csv_if_exists(path_pairs)

  extra <- c(paste0("--uids=", paste(affected_uids, collapse = ",")))
  env <- character(0)
  if (FORCE_CAP) env <- c(env, "ICEFALL_FORCE_CAP=1")
  run_rscript(script, extra, env = env)

  new_index <- read_csv_if_exists(path_index)
  new_pairs <- read_csv_if_exists(path_pairs)
  write_csv_safe(merge_cap_index(old_index, new_index), path_index)
  write_csv_safe(merge_uid_table(old_pairs, new_pairs, affected_uids), path_pairs)
}

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

msg("Project root: ", PROJECT_ROOT)
stop_if_missing(PATH_META, "main icefall table")
stop_if_missing(PATH_INPUT, "new icefall input")

meta <- read_meta(PATH_META)
input <- read_any_csv(PATH_INPUT, force_character = TRUE) %>%
  standardize_new_icefall_cols() %>%
  drop_blank_rows()

if (nrow(input) == 0) {
  stop("Input has no data rows. Fill ", PATH_INPUT, " and run again.", call. = FALSE)
}

required_input <- c("name", "latitude", "longitude")
missing_input <- setdiff(required_input, names(input))
if (length(missing_input) > 0) {
  stop("Input missing columns: ", paste(missing_input, collapse = ", "), call. = FALSE)
}

bad <- which(!nonempty(input$name) | !is.finite(to_num(input$latitude)) | !is.finite(to_num(input$longitude)))
if (length(bad) > 0) {
  stop("Input rows with missing/invalid name/latitude/longitude: ", paste(bad, collapse = ", "), call. = FALSE)
}

existing_uids <- parse_uid(meta$uid)
existing_uids <- existing_uids[is.finite(existing_uids)]
used_uids <- existing_uids
assigned_uids <- integer(nrow(input))

if (!"uid" %in% names(input)) input$uid <- NA_character_

for (i in seq_len(nrow(input))) {
  uid_i <- parse_uid(input$uid[i])
  if (is.finite(uid_i)) {
    assigned_uids[i] <- uid_i
    used_uids <- unique(c(used_uids, uid_i))
  } else {
    uid_new <- find_next_uid(used_uids)
    assigned_uids[i] <- uid_new
    used_uids <- unique(c(used_uids, uid_new))
  }
}

if (anyDuplicated(assigned_uids)) {
  stop("Duplicate UIDs in input after assignment: ", paste(assigned_uids[duplicated(assigned_uids)], collapse = ", "), call. = FALSE)
}

if (length(uid_filter) > 0) {
  keep <- assigned_uids %in% uid_filter
  input <- input[keep, , drop = FALSE]
  assigned_uids <- assigned_uids[keep]
}

if (nrow(input) == 0) stop("No input rows left after --uids filter.", call. = FALSE)

plan <- tibble(
  uid = assigned_uids,
  action = ifelse(assigned_uids %in% existing_uids, "update", "add"),
  name = as.character(input$name),
  latitude = to_num(input$latitude),
  longitude = to_num(input$longitude)
)

msg("Plan:")
print(plan)

if (DRY_RUN) {
  msg("Dry run only; no files changed.")
  quit(status = 0)
}

uid_num <- parse_uid(meta$uid)

for (i in seq_len(nrow(input))) {
  uid <- assigned_uids[i]
  row_in <- input[i, , drop = FALSE]
  row_in$uid <- as.character(uid)
  hit <- which(uid_num == uid)
  if (length(hit) > 0) {
    j <- hit[[1]]
    for (nm in intersect(names(row_in), names(meta))) {
      val <- row_in[[nm]][[1]]
      if (nonempty(val) || nm == "uid") meta[[nm]][j] <- as.character(val)
    }
  } else {
    new_row <- as.list(rep(NA_character_, length(names(meta))))
    names(new_row) <- names(meta)
    for (nm in intersect(names(row_in), names(meta))) {
      new_row[[nm]] <- as.character(row_in[[nm]][[1]])
    }
    new_row$uid <- as.character(uid)
    meta <- bind_rows(meta, as_tibble(new_row))
    uid_num <- parse_uid(meta$uid)
  }
}

affected_uids <- sort(unique(assigned_uids))
dem_catalog <- load_dem_catalog(required = TRUE)

meta <- update_height_aspect(meta, affected_uids, dem_catalog)
meta <- meta %>% arrange(parse_uid(uid))
write_semicolon_csv(meta, PATH_META)
msg("Wrote main table: ", PATH_META)

if (!SKIP_STATION) {
  msg("Update station assignment...")
  assignment_rows <- compute_station_assignments(meta, affected_uids)
  if (nrow(assignment_rows) > 0) {
    upsert_csv_by_uid(PATH_ASSIGN, assignment_rows, arrange_cols = "uid")
    msg("Wrote assignment rows: ", PATH_ASSIGN)
  }
}

if (!SKIP_WIND) {
  msg("Update wind vulnerability...")
  wind_rows <- compute_wind_lut(meta, affected_uids, dem_catalog)
  if (nrow(wind_rows) > 0) {
    upsert_csv_by_uid(PATH_WIND, wind_rows, arrange_cols = c("uid", "dir_deg"))
    msg("Wrote wind LUT: ", PATH_WIND)
  }
}

if (!SKIP_SUN) {
  msg("Update topo sun tables for ", SUN_START, " to ", SUN_END, "...")
  write_sun_tables(meta, affected_uids, dem_catalog)
}

if (!SKIP_STRUCTURE) {
  msg("Update route structure/cache tables...")
  run_structure_update(affected_uids)
}

if (!SKIP_CAP) {
  msg("Update cold-air-pooling tables...")
  run_cap_update(affected_uids)
}

if (RUN_MODELS) {
  msg("Run model plots for affected UIDs...")
  run_rscript("scripts/00_build_plots_all.R", paste0("--uids=", paste(affected_uids, collapse = ",")))
}

if (!SKIP_SITE) {
  msg("Refresh list page/table...")
  run_rscript("scripts/02_build_list_page.R")
}

if (BUILD_MAP) {
  msg("Refresh map...")
  run_rscript("scripts/01_build_map.R")
}

msg("Done. Affected UIDs: ", paste(affected_uids, collapse = ", "))
msg("Input remains in: ", PATH_INPUT)
