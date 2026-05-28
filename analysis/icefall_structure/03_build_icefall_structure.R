suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(terra)
  library(sf)
})

terraOptions(progress = 0)
sf::sf_use_s2(FALSE)

PATH_ICEFALLS <- "data/Koordinaten_Wasserfaelle/eisklettern_links_entries_diff.csv"
OUT_DIR <- "data/derived/icefall_structure"
CACHE_DIR <- file.path(OUT_DIR, "cache")
PATH_ANALYSIS <- file.path(OUT_DIR, "icefall_structure_analysis.csv")
PATH_QA <- file.path(OUT_DIR, "icefall_structure_qa.csv")
PATH_ROUTES <- file.path(OUT_DIR, "icefall_routes.geojson")

WINDOW_RADIUS_M <- 650
START_RADIUS_M <- 25
STEEP_SLOPE_DEG <- 35
UNSUPPORTED_STEEP_DEG <- 60
MAX_ROUTE_LENGTH_M <- 400
MAX_ROUTE_DROP_M <- 300
ROUTE_SAMPLE_SPACING_M <- 10
TOPO_TARGET_RES_M <- 5
TOPO_SUPPORT_RADIUS_M <- 100
CONVEXITY_SCALE_M <- 12
SUPPORT_GRADE_REF <- 0.6

DEM_SPECS <- list(
  list(
    id = "oetztal_50cm",
    label = "DOM_Oetztal_50cm",
    path = "data/DEM/DOM_Oetztal_50cm.tif",
    dem_resolution_factor = 1.10,
    coarse_target_res_m = 2,
    final_target_res_m = 0.5,
    final_corridor_buffer_m = 100,
    required = FALSE
  ),
  list(
    id = "tirol_5m",
    label = "DGM_Tirol_5m_epsg31254_2006_2020",
    path = "data/DEM/DGM_Tirol_5m_epsg31254_2006_2020.tif",
    dem_resolution_factor = 1.00,
    required = FALSE
  ),
  list(
    id = "at_5m",
    label = "DGM_AT_5m_epsg31287",
    path = "data/DEM/DGM_AT_5m_epsg31287.tif",
    dem_resolution_factor = 0.95,
    required = FALSE
  ),
  list(
    id = "eudem_25m",
    label = "eudem_dem_3035_europe",
    path = "data/DEM/eudem_dem_3035_europe.tif",
    dem_resolution_factor = 0.55,
    required = FALSE
  )
)

dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(CACHE_DIR, recursive = TRUE, showWarnings = FALSE)

args <- commandArgs(trailingOnly = TRUE)
uid_arg <- grep("^--uids=", args, value = TRUE)
uid_filter <- integer(0)
if (length(uid_arg) > 0) {
  uid_filter <- suppressWarnings(as.integer(trimws(unlist(strsplit(sub("^--uids=", "", uid_arg[[1]]), "[,;\\s]+")))))
  uid_filter <- sort(unique(uid_filter[is.finite(uid_filter)]))
}
force_cache <- any(args == "--force") || identical(Sys.getenv("ICEFALL_FORCE_CACHE", "0"), "1")

spec_num <- function(spec, name, default = NA_real_) {
  value <- spec[[name]]
  if (is.null(value)) return(default)
  value <- suppressWarnings(as.numeric(value))
  if (length(value) == 0 || !is.finite(value[[1]])) default else value[[1]]
}

clamp01 <- function(x) {
  pmax(0, pmin(1, x))
}

normalize_angle_deg <- function(x) {
  ((x %% 360) + 360) %% 360
}

opposite_angle_deg <- function(x) {
  normalize_angle_deg(x + 180)
}

angle_diff_deg <- function(a, b) {
  a <- normalize_angle_deg(a)
  b <- normalize_angle_deg(b)
  d <- abs(a - b)
  pmin(d, 360 - d)
}

bearing_deg <- function(x1, y1, x2, y2) {
  normalize_angle_deg(atan2(x2 - x1, y2 - y1) * 180 / pi)
}

replace_na_num <- function(x, value = 0) {
  x[!is.finite(x)] <- value
  x
}

safe_mean <- function(x, min_n = 1L) {
  x <- x[is.finite(x)]
  if (length(x) < min_n) return(NA_real_)
  mean(x)
}

safe_quantile <- function(x, prob) {
  x <- x[is.finite(x)]
  if (length(x) == 0) return(NA_real_)
  as.numeric(stats::quantile(x, probs = prob, na.rm = TRUE, type = 8))
}

coalesce_chr <- function(...) {
  vals <- list(...)
  for (v in vals) {
    if (length(v) == 0) next
    v <- as.character(v)
    v <- v[!is.na(v) & trimws(v) != ""]
    if (length(v) > 0) return(v[[1]])
  }
  NA_character_
}

detect_delim <- function(path) {
  header <- readLines(path, n = 1, warn = FALSE)
  if (length(header) == 0) stop("Empty file: ", path)
  counts <- c(
    ";" = stringr::str_count(header, fixed(";")),
    "," = stringr::str_count(header, fixed(",")),
    "\t" = stringr::str_count(header, fixed("\t"))
  )
  names(counts)[which.max(counts)]
}

read_any_csv <- function(path) {
  stopifnot(file.exists(path))
  readr::read_delim(
    file = path,
    delim = detect_delim(path),
    col_types = readr::cols(.default = readr::col_character()),
    show_col_types = FALSE,
    progress = FALSE
  )
}

to_num <- function(x) {
  if (is.null(x)) return(NA_real_)
  if (is.numeric(x)) return(x)
  x <- as.character(x)
  x[x %in% c("", "NA", "NaN", "NULL")] <- NA_character_
  x <- gsub(",", ".", x, fixed = TRUE)
  suppressWarnings(as.numeric(x))
}

parse_uid <- function(x) {
  as.integer(readr::parse_number(as.character(x)))
}

first_existing_col <- function(df, candidates) {
  hit <- intersect(candidates, names(df))
  if (length(hit) == 0) return(NA_character_)
  hit[[1]]
}

normalize_cardinal <- function(x) {
  x <- toupper(trimws(as.character(x)))
  x <- gsub("NORTH", "N", x, fixed = TRUE)
  x <- gsub("SOUTH", "S", x, fixed = TRUE)
  x <- gsub("EAST", "E", x, fixed = TRUE)
  x <- gsub("WEST", "W", x, fixed = TRUE)
  x <- gsub("[^NSEW]", "", x)
  x[x == "NO"] <- "NE"
  x[x == "O"] <- "E"
  x[x == "SO"] <- "SE"
  x[x == "NNO"] <- "NNE"
  x[x == "ONO"] <- "ENE"
  x[x == "OSO"] <- "ESE"
  x[x == "SSO"] <- "SSE"
  x
}

cardinal_to_degree <- function(x) {
  key <- normalize_cardinal(x)
  lut <- c(
    "N" = 0, "NNE" = 22.5, "NE" = 45, "ENE" = 67.5,
    "E" = 90, "ESE" = 112.5, "SE" = 135, "SSE" = 157.5,
    "S" = 180, "SSW" = 202.5, "SW" = 225, "WSW" = 247.5,
    "W" = 270, "WNW" = 292.5, "NW" = 315, "NNW" = 337.5
  )
  out <- unname(lut[key])
  out[is.na(out)] <- NA_real_
  as.numeric(out)
}

is_degree_consistent_with_cardinal <- function(deg, cardinal) {
  target <- cardinal_to_degree(cardinal)
  is.finite(deg) & is.finite(target) & angle_diff_deg(deg, target) <= 45
}

parse_height_hint_m <- function(x) {
  x <- as.character(x)
  match <- stringr::str_match(x, "(\\d+[\\.,]?\\d*)\\s*m\\b")
  to_num(match[, 2])
}

weighted_smooth_coords <- function(coords) {
  coords <- as.matrix(coords)
  if (nrow(coords) < 3) return(coords)
  out <- coords
  for (i in 2:(nrow(coords) - 1)) {
    out[i, ] <- (coords[i - 1, ] + 2 * coords[i, ] + coords[i + 1, ]) / 4
  }
  out
}

dedupe_coords <- function(coords, tol = 1e-8) {
  coords <- as.matrix(coords)
  if (nrow(coords) < 2) return(coords)
  keep <- c(TRUE, sqrt(rowSums((coords[-1, , drop = FALSE] - coords[-nrow(coords), , drop = FALSE])^2)) > tol)
  coords[keep, , drop = FALSE]
}

resample_polyline <- function(coords, spacing = ROUTE_SAMPLE_SPACING_M) {
  coords <- dedupe_coords(coords)
  if (nrow(coords) < 2) return(coords)
  seg_len <- sqrt(rowSums((coords[-1, , drop = FALSE] - coords[-nrow(coords), , drop = FALSE])^2))
  total <- sum(seg_len)
  if (!is.finite(total) || total <= 0) return(coords[1, , drop = FALSE])
  stations <- c(0, cumsum(seg_len))
  target <- unique(c(seq(0, total, by = spacing), total))
  x <- stats::approx(stations, coords[, 1], xout = target, ties = "ordered")$y
  y <- stats::approx(stations, coords[, 2], xout = target, ties = "ordered")$y
  cbind(x = x, y = y)
}

line_length_m <- function(coords) {
  coords <- as.matrix(coords)
  if (nrow(coords) < 2) return(0)
  sum(sqrt(rowSums((coords[-1, , drop = FALSE] - coords[-nrow(coords), , drop = FALSE])^2)))
}

local_topo_position <- function(dem, pts, radius_m = TOPO_SUPPORT_RADIUS_M) {
  out <- rep(NA_real_, nrow(pts))
  for (i in seq_len(nrow(pts))) {
    pt <- pts[i]
    z0 <- suppressWarnings(terra::extract(dem, pt)[1, 2])
    if (!is.finite(z0)) next
    buf <- terra::buffer(pt, width = radius_m)
    vals <- terra::values(terra::mask(terra::crop(dem, buf), buf), mat = FALSE, na.rm = TRUE)
    vals <- vals[is.finite(vals)]
    if (length(vals) < 5) next
    out[i] <- mean(vals <= z0)
  }
  out
}

get_neighbor_cells <- function(r, cell) {
  rc <- terra::rowColFromCell(r, cell)
  rr <- seq(max(1, rc[1] - 1), min(nrow(r), rc[1] + 1))
  cc <- seq(max(1, rc[2] - 1), min(ncol(r), rc[2] + 1))
  grid <- expand.grid(row = rr, col = cc)
  grid <- grid[!(grid$row == rc[1] & grid$col == rc[2]), , drop = FALSE]
  terra::cellFromRowCol(r, row = grid$row, col = grid$col)
}

is_edge_cell <- function(r, cell) {
  rc <- terra::rowColFromCell(r, cell)
  rc[1] <= 1 || rc[1] >= nrow(r) || rc[2] <= 1 || rc[2] >= ncol(r)
}

project_xy_to_ll <- function(coords, crs_from) {
  pts <- sf::st_sfc(lapply(seq_len(nrow(coords)), function(i) sf::st_point(coords[i, ])), crs = crs_from)
  pts_ll <- sf::st_transform(pts, 4326)
  do.call(rbind, lapply(sf::st_geometry(pts_ll), unclass))
}

build_meta <- function(path) {
  raw <- read_any_csv(path) %>%
    rename_with(tolower)
  lat_col <- first_existing_col(raw, c("latitude", "lat"))
  lon_col <- first_existing_col(raw, c("longitude", "lon"))
  if (is.na(lat_col) || is.na(lon_col)) {
    stop("Could not find latitude/longitude columns in ", path)
  }
  aspect_num <- to_num(raw$aspect_deg)
  aspect_cardinal <- normalize_cardinal(raw$aspect_cardinal)
  aspect_pref <- aspect_num
  use_cardinal <- !is.na(aspect_cardinal) & (is.na(aspect_pref) | !is_degree_consistent_with_cardinal(aspect_pref, aspect_cardinal))
  aspect_pref[use_cardinal] <- cardinal_to_degree(aspect_cardinal[use_cardinal])
  tibble(
    uid = parse_uid(raw$uid),
    name = as.character(raw$name),
    latitude = to_num(raw[[lat_col]]),
    longitude = to_num(raw[[lon_col]]),
    elev_m = to_num(raw$elevation_dgm5m),
    difficulty = as.character(raw$difficulty),
    height_hint_m = parse_height_hint_m(raw$icefall_height_m),
    source_aspect_deg = aspect_num,
    aspect_cardinal = as.character(aspect_cardinal),
    preferred_aspect_deg = aspect_pref,
    topo_url = as.character(raw$topo_url)
  ) %>%
    filter(!is.na(uid)) %>%
    group_by(uid) %>%
    slice(1) %>%
    ungroup()
}

load_dem_catalog <- function() {
  out <- lapply(DEM_SPECS, function(spec) {
    if (!file.exists(spec$path)) {
      if (isTRUE(spec$required)) stop("Missing DEM: ", spec$path)
      message("DEM not found, skip: ", spec$path)
      return(NULL)
    }
    r <- terra::rast(spec$path)
    spec$resolution_m <- mean(terra::res(r))
    list(spec = spec, raster = r)
  })
  out <- Filter(Negate(is.null), out)
  if (length(out) == 0) stop("No DEM files found.")
  out
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

build_local_bundle <- function(uid, dem_choice, force = FALSE, mode = "single", crop_ext = NULL, target_res_m = NA_real_) {
  uid_key <- sprintf("%03d", as.integer(uid))
  stem <- paste0("uid_", uid_key, "_", dem_choice$spec$id, "_", mode)
  path_dem <- file.path(CACHE_DIR, paste0(stem, "_dem.tif"))
  path_slope <- file.path(CACHE_DIR, paste0(stem, "_slope.tif"))
  path_aspect <- file.path(CACHE_DIR, paste0(stem, "_aspect.tif"))
  path_topo <- file.path(CACHE_DIR, paste0(stem, "_topo.tif"))
  path_tpi <- file.path(CACHE_DIR, paste0(stem, "_tpi.tif"))

  cache_exists <- all(file.exists(c(path_dem, path_slope, path_aspect, path_topo, path_tpi)))
  if (!cache_exists || force) {
    if (is.null(crop_ext)) {
      buf <- terra::buffer(dem_choice$point, width = WINDOW_RADIUS_M)
      crop_ext <- terra::ext(buf)
    }
    dem_local <- terra::crop(dem_choice$raster, crop_ext, snap = "out")
    if (is.null(dem_local) || terra::ncell(dem_local) == 0) {
      stop("No DEM cells in local window")
    }
    target_res <- target_res_m
    if (!is.finite(target_res)) {
      target_res <- spec_num(dem_choice$spec, "structure_target_res_m", NA_real_)
    }
    if (is.finite(target_res)) {
      source_res <- mean(terra::res(dem_local))
      if (is.finite(source_res) && source_res < target_res) {
        fact <- max(1L, round(target_res / source_res))
        message(
          "  UID ", uid, ": ", mode, " ", dem_choice$spec$id,
          " window from ", round(source_res, 2),
          " m to about ", round(source_res * fact, 2), " m"
        )
        dem_local <- terra::aggregate(dem_local, fact = fact, fun = mean, na.rm = TRUE)
      }
    }
    names(dem_local) <- "elev_m"
    if (!any(is.finite(terra::values(dem_local, mat = FALSE)))) {
      stop("Local DEM window only contains NA values")
    }
    slope_aspect <- terra::terrain(dem_local, v = c("slope", "aspect"), unit = "degrees", neighbors = 8)
    slope_local <- slope_aspect[[1]]
    aspect_local <- slope_aspect[[2]]
    names(slope_local) <- "slope_deg"
    names(aspect_local) <- "aspect_deg"

    fact_xy <- max(1L, round(TOPO_TARGET_RES_M / mean(terra::res(dem_local))))
    topo_local <- if (fact_xy > 1L) {
      terra::aggregate(dem_local, fact = fact_xy, fun = mean, na.rm = TRUE)
    } else {
      dem_local
    }
    topo_local <- terra::focal(
      topo_local,
      w = matrix(1, nrow = 3, ncol = 3),
      fun = mean,
      na.policy = "omit",
      fillvalue = NA
    )
    names(topo_local) <- "topo_m"
    tpi_local <- terra::terrain(topo_local, v = "TPI")
    names(tpi_local) <- "tpi_m"

    terra::writeRaster(dem_local, path_dem, overwrite = TRUE)
    terra::writeRaster(slope_local, path_slope, overwrite = TRUE)
    terra::writeRaster(aspect_local, path_aspect, overwrite = TRUE)
    terra::writeRaster(topo_local, path_topo, overwrite = TRUE)
    terra::writeRaster(tpi_local, path_tpi, overwrite = TRUE)
  }

  list(
    dem = terra::rast(path_dem),
    slope = terra::rast(path_slope),
    aspect = terra::rast(path_aspect),
    topo = terra::rast(path_topo),
    tpi = terra::rast(path_tpi)
  )
}

route_corridor_ext <- function(route_coords, buffer_m) {
  terra::ext(
    min(route_coords[, 1], na.rm = TRUE) - buffer_m,
    max(route_coords[, 1], na.rm = TRUE) + buffer_m,
    min(route_coords[, 2], na.rm = TRUE) - buffer_m,
    max(route_coords[, 2], na.rm = TRUE) + buffer_m
  )
}

find_start_cell <- function(dem_local, slope_local, point_proj, radius_m = START_RADIUS_M) {
  xy <- terra::crds(point_proj, df = TRUE)[1, c("x", "y")]
  e <- terra::ext(xy$x - radius_m, xy$x + radius_m, xy$y - radius_m, xy$y + radius_m)
  dem_small <- tryCatch(terra::crop(dem_local, e, snap = "out"), error = function(e) NULL)
  slope_small <- tryCatch(terra::crop(slope_local, e, snap = "out"), error = function(e) NULL)
  if (is.null(dem_small) || is.null(slope_small) || terra::ncell(dem_small) == 0) return(NA_integer_)
  df <- terra::as.data.frame(c(dem_small, slope_small), xy = TRUE, cells = TRUE, na.rm = FALSE)
  if (nrow(df) == 0) return(NA_integer_)
  df <- df[is.finite(df$elev_m), , drop = FALSE]
  if (nrow(df) == 0) return(NA_integer_)
  df$dist_m <- sqrt((df$x - xy$x)^2 + (df$y - xy$y)^2)
  df <- df[df$dist_m <= radius_m, , drop = FALSE]
  if (nrow(df) == 0) return(NA_integer_)
  slope_rank <- if ("slope_deg" %in% names(df)) replace_na_num(df$slope_deg, 0) else rep(0, nrow(df))
  df <- df[order(df$dist_m, -slope_rank), , drop = FALSE]
  terra::cellFromXY(dem_local, as.matrix(df[1, c("x", "y"), drop = FALSE]))
}

trace_route_direction <- function(start_cell, dem_local, slope_local, aspect_local, preferred_down_deg, direction = c("down", "up")) {
  direction <- match.arg(direction)
  dem_vals <- terra::values(dem_local, mat = FALSE)
  slope_vals <- terra::values(slope_local, mat = FALSE)
  aspect_vals <- terra::values(aspect_local, mat = FALSE)
  xy_vals <- terra::crds(dem_local, df = TRUE)

  route <- c(start_cell)
  current_cell <- start_cell
  start_z <- dem_vals[start_cell]
  traveled_m <- 0
  prev_angle <- NA_real_
  below_counter <- if (is.finite(slope_vals[start_cell]) && slope_vals[start_cell] < STEEP_SLOPE_DEG) 1L else 0L

  repeat {
    neigh <- get_neighbor_cells(dem_local, current_cell)
    neigh <- neigh[is.finite(dem_vals[neigh])]
    neigh <- neigh[!(neigh %in% route)]
    if (length(neigh) == 0) break

    current_xy <- xy_vals[current_cell, c("x", "y"), drop = FALSE]
    neigh_xy <- xy_vals[neigh, c("x", "y"), drop = FALSE]
    move_angles <- bearing_deg(current_xy$x, current_xy$y, neigh_xy$x, neigh_xy$y)
    move_dist <- sqrt((neigh_xy$x - current_xy$x)^2 + (neigh_xy$y - current_xy$y)^2)
    current_aspect <- aspect_vals[current_cell]
    local_target <- if (is.finite(current_aspect)) current_aspect else preferred_down_deg
    if (direction == "up") local_target <- opposite_angle_deg(local_target)
    global_target <- if (direction == "down") preferred_down_deg else opposite_angle_deg(preferred_down_deg)

    local_align <- clamp01(1 - angle_diff_deg(move_angles, local_target) / 60)
    global_align <- clamp01(1 - angle_diff_deg(move_angles, global_target) / 60)
    continuity <- if (is.finite(prev_angle)) clamp01(1 - angle_diff_deg(move_angles, prev_angle) / 90) else rep(1, length(neigh))
    slope_score <- clamp01(replace_na_num(slope_vals[neigh], 0) / 80)
    elev_delta <- dem_vals[neigh] - dem_vals[current_cell]
    direction_bonus <- if (direction == "down") ifelse(elev_delta < 0, 1, 0) else ifelse(elev_delta > 0, 1, 0)

    total_score <- 0.32 * slope_score +
      0.24 * local_align +
      0.18 * continuity +
      0.16 * global_align +
      0.10 * direction_bonus

    if (any(is.finite(slope_vals[neigh]) & slope_vals[neigh] >= STEEP_SLOPE_DEG)) {
      total_score[!is.finite(slope_vals[neigh]) | slope_vals[neigh] < STEEP_SLOPE_DEG] <-
        total_score[!is.finite(slope_vals[neigh]) | slope_vals[neigh] < STEEP_SLOPE_DEG] - 0.15
    }

    candidate_order <- order(total_score, replace_na_num(slope_vals[neigh], -1), decreasing = TRUE, na.last = NA)
    if (length(candidate_order) == 0) break

    picked <- FALSE
    for (idx in candidate_order) {
      next_cell <- neigh[idx]
      next_length <- traveled_m + move_dist[idx]
      next_drop <- abs(dem_vals[next_cell] - start_z)
      if (next_length > MAX_ROUTE_LENGTH_M) next
      if (next_drop > MAX_ROUTE_DROP_M) next
      if (is_edge_cell(dem_local, next_cell)) next
      route <- c(route, next_cell)
      current_cell <- next_cell
      traveled_m <- next_length
      prev_angle <- move_angles[idx]
      below_counter <- if (is.finite(slope_vals[next_cell]) && slope_vals[next_cell] < STEEP_SLOPE_DEG) below_counter + 1L else 0L
      picked <- TRUE
      break
    }
    if (!picked) break
    if (below_counter >= 3L) break
  }

  unique(route)
}

build_route_coords <- function(start_cell, dem_local, slope_local, aspect_local, preferred_down_deg) {
  up_cells <- trace_route_direction(start_cell, dem_local, slope_local, aspect_local, preferred_down_deg, direction = "up")
  down_cells <- trace_route_direction(start_cell, dem_local, slope_local, aspect_local, preferred_down_deg, direction = "down")
  all_cells <- c(rev(up_cells), down_cells[-1])
  all_cells <- all_cells[is.finite(all_cells)]
  all_cells <- all_cells[!duplicated(all_cells)]
  coords <- terra::crds(dem_local, df = TRUE)[all_cells, c("x", "y"), drop = FALSE]
  coords <- dedupe_coords(as.matrix(coords))
  coords <- weighted_smooth_coords(coords)
  coords <- resample_polyline(coords, spacing = ROUTE_SAMPLE_SPACING_M)
  dedupe_coords(coords)
}

extract_raster_values <- function(r, coords, crs_value) {
  if (nrow(coords) == 0) return(numeric(0))
  pts <- terra::vect(data.frame(x = coords[, 1], y = coords[, 2]), geom = c("x", "y"), crs = crs_value)
  as.numeric(terra::extract(r, pts)[, 2])
}

compute_confinement <- function(route_coords, dem_local, crs_value) {
  if (nrow(route_coords) < 3) {
    return(list(index = NA_real_, per_sample = rep(NA_real_, nrow(route_coords))))
  }
  n <- nrow(route_coords)
  tangents <- rep(NA_real_, n)
  for (i in seq_len(n)) {
    i_prev <- max(1, i - 1)
    i_next <- min(n, i + 1)
    if (i_prev == i_next) next
    tangents[i] <- bearing_deg(
      route_coords[i_prev, 1], route_coords[i_prev, 2],
      route_coords[i_next, 1], route_coords[i_next, 2]
    )
  }

  center_vals <- extract_raster_values(dem_local, route_coords, crs_value)
  side_table <- expand.grid(sample_id = seq_len(n), offset_m = c(10, 20), side = c(-1, 1))
  normal_deg <- normalize_angle_deg(tangents[side_table$sample_id] + ifelse(side_table$side < 0, -90, 90))
  normal_rad <- normal_deg * pi / 180
  side_table$x <- route_coords[side_table$sample_id, 1] + sin(normal_rad) * side_table$offset_m
  side_table$y <- route_coords[side_table$sample_id, 2] + cos(normal_rad) * side_table$offset_m
  side_vals <- extract_raster_values(dem_local, as.matrix(side_table[, c("x", "y")]), crs_value)
  side_table$side_z <- side_vals
  side_table$center_z <- center_vals[side_table$sample_id]
  side_table$support_component <- clamp01(
    pmax(0, side_table$side_z - side_table$center_z) / (SUPPORT_GRADE_REF * side_table$offset_m)
  )

  sample_conf <- rep(NA_real_, n)
  for (i in seq_len(n)) {
    sample_rows <- side_table[side_table$sample_id == i, , drop = FALSE]
    offset_scores <- vapply(
      c(10, 20),
      function(offset_value) {
        vals <- sample_rows$support_component[sample_rows$offset_m == offset_value]
        if (length(vals) == 0 || !any(is.finite(vals))) return(NA_real_)
        max(vals, na.rm = TRUE)
      },
      numeric(1)
    )
    sample_conf[i] <- if (any(is.finite(offset_scores))) max(offset_scores, na.rm = TRUE) else NA_real_
  }
  list(
    index = safe_mean(sample_conf, min_n = max(2L, floor(0.6 * n))),
    per_sample = sample_conf
  )
}

analyze_single_icefall <- function(row, dem_catalog, force_local_cache = FALSE) {
  base <- list(
    uid = as.integer(row$uid),
    name = coalesce_chr(row$name),
    latitude = to_num(row$latitude),
    longitude = to_num(row$longitude),
    source_aspect_deg = to_num(row$source_aspect_deg),
    aspect_cardinal = coalesce_chr(row$aspect_cardinal),
    preferred_aspect_deg = to_num(row$preferred_aspect_deg),
    elev_m = to_num(row$elev_m),
    difficulty = coalesce_chr(row$difficulty),
    height_hint_m = to_num(row$height_hint_m),
    dem_source = NA_character_,
    dem_resolution_m = NA_real_,
    line_status = NA_character_,
    route_point_count = NA_integer_,
    route_length_m = NA_real_,
    route_drop_m = NA_real_,
    route_bearing_deg = NA_real_,
    slope_mean_deg = NA_real_,
    slope_p90_deg = NA_real_,
    slope_max_deg = NA_real_,
    aspect_agreement = NA_real_,
    steep_continuity = NA_real_,
    dem_resolution_factor = NA_real_,
    route_length_factor = NA_real_,
    route_confidence = NA_real_,
    confinement_index = NA_real_,
    topo_support_index = NA_real_,
    convexity_index = NA_real_,
    unsupported_steep_fraction = NA_real_,
    support_score = NA_real_,
    collapse_risk_score = NA_real_,
    qa_flag = NA_character_,
    qa_note = NA_character_
  )

  if (!is.finite(base$latitude) || !is.finite(base$longitude)) {
    base$line_status <- "qa_only"
    base$qa_flag <- "missing_coordinates"
    base$qa_note <- "Icefall entry has no valid latitude/longitude."
    return(list(record = base, route_coords_ll = NULL, route_coords_local = NULL, accepted = FALSE))
  }

  point_ll <- terra::vect(
    data.frame(x = base$longitude, y = base$latitude),
    geom = c("x", "y"),
    crs = "EPSG:4326"
  )

  dem_choice <- choose_dem_for_point(point_ll, dem_catalog)
  if (is.null(dem_choice)) {
    base$line_status <- "qa_only"
    base$qa_flag <- "no_dem_hit"
    base$qa_note <- "No DEM returned a valid elevation for the icefall point."
    return(list(record = base, route_coords_ll = NULL, route_coords_local = NULL, accepted = FALSE))
  }

  base$dem_source <- dem_choice$spec$label
  base$dem_resolution_m <- dem_choice$spec$resolution_m
  base$dem_resolution_factor <- dem_choice$spec$dem_resolution_factor

  coarse_target_res <- spec_num(dem_choice$spec, "coarse_target_res_m", NA_real_)
  local_bundle <- build_local_bundle(
    base$uid,
    dem_choice,
    force = force_local_cache,
    mode = if (is.finite(coarse_target_res)) "coarse" else "single",
    target_res_m = coarse_target_res
  )
  if (!is.finite(base$preferred_aspect_deg)) {
    point_aspect <- suppressWarnings(as.numeric(terra::extract(local_bundle$aspect, dem_choice$point)[1, 2]))
    base$preferred_aspect_deg <- point_aspect
  }
  if (!is.finite(base$preferred_aspect_deg)) {
    base$line_status <- "qa_only"
    base$qa_flag <- "missing_aspect"
    base$qa_note <- "No source or raster aspect was available at the icefall point."
    return(list(record = base, route_coords_ll = NULL, route_coords_local = NULL, accepted = FALSE))
  }

  start_cell <- find_start_cell(local_bundle$dem, local_bundle$slope, dem_choice$point, radius_m = START_RADIUS_M)
  if (!is.finite(start_cell)) {
    base$line_status <- "qa_only"
    base$qa_flag <- "no_start_cell"
    base$qa_note <- "No valid DEM cell was found within 25 m of the input point."
    return(list(record = base, route_coords_ll = NULL, route_coords_local = NULL, accepted = FALSE))
  }

  route_coords <- build_route_coords(start_cell, local_bundle$dem, local_bundle$slope, local_bundle$aspect, base$preferred_aspect_deg)
  if (nrow(route_coords) < 3) {
    base$line_status <- "qa_only"
    base$qa_flag <- "route_failed"
    base$qa_note <- "Automatic route tracing could not build a line with at least three support points."
    return(list(record = base, route_coords_ll = NULL, route_coords_local = route_coords, accepted = FALSE))
  }

  final_target_res <- spec_num(dem_choice$spec, "final_target_res_m", NA_real_)
  final_corridor_buffer <- spec_num(dem_choice$spec, "final_corridor_buffer_m", NA_real_)
  if (is.finite(final_target_res) && is.finite(final_corridor_buffer) && final_corridor_buffer > 0) {
    message(
      "  UID ", base$uid, ": final 50 cm structure pass in ",
      round(final_corridor_buffer), " m corridor"
    )
    final_bundle <- tryCatch(
      build_local_bundle(
        base$uid,
        dem_choice,
        force = force_local_cache,
        mode = "final",
        crop_ext = route_corridor_ext(route_coords, final_corridor_buffer),
        target_res_m = final_target_res
      ),
      error = function(e) {
        message("  UID ", base$uid, ": final pass failed; keep coarse route: ", conditionMessage(e))
        NULL
      }
    )
    if (!is.null(final_bundle)) {
      final_start_cell <- find_start_cell(final_bundle$dem, final_bundle$slope, dem_choice$point, radius_m = START_RADIUS_M)
      if (is.finite(final_start_cell)) {
        final_route_coords <- build_route_coords(
          final_start_cell,
          final_bundle$dem,
          final_bundle$slope,
          final_bundle$aspect,
          base$preferred_aspect_deg
        )
        if (nrow(final_route_coords) >= 3 && line_length_m(final_route_coords) > 0) {
          local_bundle <- final_bundle
          route_coords <- final_route_coords
        } else {
          message("  UID ", base$uid, ": final route too short; keep coarse route.")
        }
      } else {
        message("  UID ", base$uid, ": no final start cell; keep coarse route.")
      }
    }
  }

  base$dem_resolution_m <- mean(terra::res(local_bundle$dem))

  route_length <- line_length_m(route_coords)
  if (!is.finite(route_length) || route_length <= 0) {
    base$line_status <- "qa_only"
    base$qa_flag <- "zero_length_route"
    base$qa_note <- "Automatic route tracing produced a zero-length line."
    return(list(record = base, route_coords_ll = NULL, route_coords_local = route_coords, accepted = FALSE))
  }

  route_z <- extract_raster_values(local_bundle$dem, route_coords, terra::crs(local_bundle$dem))
  bearing_a <- bearing_deg(route_coords[1, 1], route_coords[1, 2], route_coords[nrow(route_coords), 1], route_coords[nrow(route_coords), 2])
  bearing_b <- opposite_angle_deg(bearing_a)
  if (angle_diff_deg(bearing_a, base$preferred_aspect_deg) > angle_diff_deg(bearing_b, base$preferred_aspect_deg)) {
    route_coords <- route_coords[nrow(route_coords):1, , drop = FALSE]
    route_z <- rev(route_z)
  }

  route_bearing <- bearing_deg(route_coords[1, 1], route_coords[1, 2], route_coords[nrow(route_coords), 1], route_coords[nrow(route_coords), 2])
  slope_vals <- extract_raster_values(local_bundle$slope, route_coords, terra::crs(local_bundle$dem))
  topo_pos <- local_topo_position(
    local_bundle$topo,
    terra::vect(data.frame(x = route_coords[, 1], y = route_coords[, 2]), geom = c("x", "y"), crs = terra::crs(local_bundle$dem)),
    radius_m = TOPO_SUPPORT_RADIUS_M
  )
  tpi_vals <- extract_raster_values(local_bundle$tpi, route_coords, terra::crs(local_bundle$dem))
  conf <- compute_confinement(route_coords, local_bundle$dem, terra::crs(local_bundle$dem))

  base$route_point_count <- nrow(route_coords)
  base$route_length_m <- route_length
  base$route_drop_m <- max(route_z, na.rm = TRUE) - min(route_z, na.rm = TRUE)
  base$route_bearing_deg <- route_bearing
  base$slope_mean_deg <- safe_mean(slope_vals, min_n = max(3L, floor(0.6 * length(slope_vals))))
  base$slope_p90_deg <- safe_quantile(slope_vals, 0.90)
  base$slope_max_deg <- if (any(is.finite(slope_vals))) max(slope_vals, na.rm = TRUE) else NA_real_
  base$aspect_agreement <- clamp01(1 - angle_diff_deg(route_bearing, base$preferred_aspect_deg) / 90)
  base$steep_continuity <- safe_mean(as.numeric(slope_vals >= STEEP_SLOPE_DEG), min_n = max(3L, floor(0.6 * length(slope_vals))))
  base$route_length_factor <- clamp01(base$route_length_m / 160)
  base$confinement_index <- conf$index
  base$topo_support_index <- safe_mean(1 - topo_pos, min_n = max(3L, floor(0.6 * length(topo_pos))))
  base$convexity_index <- safe_mean(clamp01(pmax(0, tpi_vals) / CONVEXITY_SCALE_M), min_n = max(3L, floor(0.6 * length(tpi_vals))))
  base$unsupported_steep_fraction <- safe_mean(as.numeric(slope_vals >= UNSUPPORTED_STEEP_DEG & conf$per_sample < 0.35), min_n = max(3L, floor(0.6 * length(slope_vals))))
  base$route_confidence <- 0.40 * base$aspect_agreement +
    0.30 * base$steep_continuity +
    0.20 * base$dem_resolution_factor +
    0.10 * base$route_length_factor
  if (identical(dem_choice$spec$id, "eudem_25m")) {
    base$route_confidence <- min(base$route_confidence, 0.79)
  }

  components <- c(base$confinement_index, base$topo_support_index, base$convexity_index, base$unsupported_steep_fraction)
  if (!all(is.finite(components))) {
    base$line_status <- "qa_only"
    base$qa_flag <- "support_metrics_incomplete"
    base$qa_note <- "Route traced, but support metrics were too incomplete to score reliably."
  } else {
    base$support_score <- 100 * (
      0.45 * base$confinement_index +
      0.25 * base$topo_support_index +
      0.20 * (1 - base$convexity_index) +
      0.10 * (1 - base$unsupported_steep_fraction)
    )
    base$support_score <- max(0, min(100, base$support_score))
    base$collapse_risk_score <- 100 - base$support_score

    if (!is.finite(base$route_confidence) || base$route_confidence < 0.40) {
      base$line_status <- "low_confidence"
      base$qa_flag <- "low_confidence"
      base$qa_note <- "Automatic route was generated, but confidence stayed below 0.40."
    } else {
      base$line_status <- "ok"
    }
  }

  route_coords_ll <- project_xy_to_ll(route_coords, terra::crs(local_bundle$dem))
  accepted <- identical(base$line_status, "ok")

  list(
    record = base,
    route_coords_ll = route_coords_ll,
    route_coords_local = route_coords,
    accepted = accepted
  )
}

message("Loading icefall metadata...")
meta <- build_meta(PATH_ICEFALLS)
if (length(uid_filter) > 0) {
  meta <- meta %>% filter(uid %in% uid_filter)
  message("UID filter active: ", paste(uid_filter, collapse = ", "))
}

message("Input icefalls: ", nrow(meta))
message("Loading DEM catalog...")
dem_catalog <- load_dem_catalog()

records_analysis <- vector("list", nrow(meta))
records_qa <- vector("list", nrow(meta))
route_geoms <- vector("list", nrow(meta))
route_props <- vector("list", nrow(meta))
analysis_i <- 0L
qa_i <- 0L
route_i <- 0L

for (i in seq_len(nrow(meta))) {
  row <- meta[i, , drop = FALSE]
  uid <- as.integer(row$uid[[1]])
  message(sprintf("[%d/%d] uid=%s %s", i, nrow(meta), uid, coalesce_chr(row$name[[1]])))
  result <- tryCatch(
    analyze_single_icefall(row, dem_catalog, force_local_cache = force_cache),
    error = function(e) {
      list(
        record = list(
          uid = uid,
          name = coalesce_chr(row$name[[1]]),
          latitude = to_num(row$latitude[[1]]),
          longitude = to_num(row$longitude[[1]]),
          source_aspect_deg = to_num(row$source_aspect_deg[[1]]),
          aspect_cardinal = coalesce_chr(row$aspect_cardinal[[1]]),
          preferred_aspect_deg = to_num(row$preferred_aspect_deg[[1]]),
          elev_m = to_num(row$elev_m[[1]]),
          difficulty = coalesce_chr(row$difficulty[[1]]),
          height_hint_m = to_num(row$height_hint_m[[1]]),
          dem_source = NA_character_,
          dem_resolution_m = NA_real_,
          line_status = "qa_only",
          route_point_count = NA_integer_,
          route_length_m = NA_real_,
          route_drop_m = NA_real_,
          route_bearing_deg = NA_real_,
          slope_mean_deg = NA_real_,
          slope_p90_deg = NA_real_,
          slope_max_deg = NA_real_,
          aspect_agreement = NA_real_,
          steep_continuity = NA_real_,
          dem_resolution_factor = NA_real_,
          route_length_factor = NA_real_,
          route_confidence = NA_real_,
          confinement_index = NA_real_,
          topo_support_index = NA_real_,
          convexity_index = NA_real_,
          unsupported_steep_fraction = NA_real_,
          support_score = NA_real_,
          collapse_risk_score = NA_real_,
          qa_flag = "analysis_error",
          qa_note = conditionMessage(e)
        ),
        route_coords_ll = NULL,
        route_coords_local = NULL,
        accepted = FALSE
      )
    }
  )

  if (!is.null(result$route_coords_ll) && nrow(result$route_coords_ll) >= 3) {
    route_i <- route_i + 1L
    route_geoms[[route_i]] <- sf::st_linestring(as.matrix(result$route_coords_ll))
    route_props[[route_i]] <- tibble(
      uid = result$record$uid,
      name = result$record$name,
      dem_source = result$record$dem_source,
      line_status = result$record$line_status,
      route_confidence = result$record$route_confidence,
      support_score = result$record$support_score,
      collapse_risk_score = result$record$collapse_risk_score
    )
  }

  if (isTRUE(result$accepted)) {
    analysis_i <- analysis_i + 1L
    records_analysis[[analysis_i]] <- tibble::as_tibble(result$record)
  } else {
    qa_i <- qa_i + 1L
    records_qa[[qa_i]] <- tibble::as_tibble(result$record)
  }
}

analysis_df <- dplyr::bind_rows(records_analysis[seq_len(analysis_i)])
qa_df <- dplyr::bind_rows(records_qa[seq_len(qa_i)])

if (route_i > 0L) {
  routes_sf <- sf::st_sf(
    dplyr::bind_rows(route_props[seq_len(route_i)]),
    geometry = sf::st_sfc(route_geoms[seq_len(route_i)], crs = 4326)
  )
  if (file.exists(PATH_ROUTES)) unlink(PATH_ROUTES)
  suppressWarnings(sf::st_write(routes_sf, PATH_ROUTES, quiet = TRUE, delete_dsn = TRUE))
} else {
  if (file.exists(PATH_ROUTES)) unlink(PATH_ROUTES)
}

readr::write_csv(analysis_df, PATH_ANALYSIS, na = "")
readr::write_csv(qa_df, PATH_QA, na = "")

count_geo_rows <- function(df) {
  if (!all(c("latitude", "longitude") %in% names(df))) return(0L)
  sum(is.finite(df$latitude) & is.finite(df$longitude))
}

has_coords <- is.finite(meta$latitude) & is.finite(meta$longitude)
accepted_geo <- count_geo_rows(analysis_df)
qa_geo <- count_geo_rows(qa_df)
message("Accepted analysis rows: ", nrow(analysis_df))
message("QA rows: ", nrow(qa_df))
message("GeoJSON routes: ", route_i)
message("Coverage check (geo input vs accepted+qa): ", sum(has_coords), " vs ", accepted_geo + qa_geo)
message("Outputs written to: ", OUT_DIR)
