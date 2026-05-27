suppressPackageStartupMessages({
  library(httr2)
  library(dplyr)
  library(lubridate)
  library(tidyr)
  library(readr)
  library(ggplot2)
  library(zoo)
})

TZ_LOCAL <- "Europe/Vienna"
MODEL_STEP_MIN <- 10
DT_H <- MODEL_STEP_MIN / 60
DT_SEC <- MODEL_STEP_MIN * 60
W2MJ_STEP <- DT_SEC / 1e6

TARGET_UID <- 54L
FIRST_SEASON_START_YEAR <- 1993L

args_all <- commandArgs(trailingOnly = FALSE)
file_arg <- "--file="
script_path <- sub(file_arg, "", args_all[grepl(file_arg, args_all)])
script_dir <- if (length(script_path) >= 1) {
  dirname(normalizePath(script_path[[1]], winslash = "/", mustWork = TRUE))
} else {
  normalizePath(getwd(), winslash = "/", mustWork = TRUE)
}
root_candidates <- unique(c(
  script_dir,
  normalizePath(file.path(script_dir, ".."), winslash = "/", mustWork = FALSE),
  normalizePath(file.path(script_dir, "..", ".."), winslash = "/", mustWork = FALSE),
  normalizePath(file.path(script_dir, "..", "..", ".."), winslash = "/", mustWork = FALSE),
  normalizePath(getwd(), winslash = "/", mustWork = TRUE)
))
root_dir <- root_candidates[
  vapply(
    root_candidates,
    function(x) file.exists(file.path(x, "data", "AWS", "icefalls_nearest_station.csv")),
    logical(1)
  )
][1]
if (is.na(root_dir) || !nzchar(root_dir)) {
  stop("Could not detect repository root from script path or working directory.")
}
root_dir <- normalizePath(root_dir, winslash = "/", mustWork = TRUE)

case_dir <- file.path(root_dir, "analysis", "historical_climate", "Amphitrite_Brenner")
dir_cache <- file.path(case_dir, "cache")
dir_output <- file.path(case_dir, "output")
dir.create(dir_cache, recursive = TRUE, showWarnings = FALSE)
dir.create(dir_output, recursive = TRUE, showWarnings = FALSE)

path_assign <- file.path(root_dir, "data", "AWS", "icefalls_nearest_station.csv")
path_stations <- file.path(root_dir, "data", "AWS", "stations_all.csv")
path_structure <- file.path(root_dir, "data", "derived", "icefall_structure", "icefall_structure_analysis.csv")
path_wind_lut <- file.path(root_dir, "data", "Wind", "wind_vulnerability_5deg.csv")

`%||%` <- function(a, b) if (!is.null(a)) a else b

to_num <- function(x) {
  if (is.numeric(x)) return(x)
  x <- as.character(x)
  x <- gsub(",", ".", x, fixed = TRUE)
  suppressWarnings(as.numeric(x))
}

fill1 <- function(x) {
  x <- zoo::na.approx(x, na.rm = FALSE)
  x <- zoo::na.locf(x, na.rm = FALSE)
  x <- zoo::na.locf(x, na.rm = FALSE, fromLast = TRUE)
  x
}

clamp01 <- function(x) pmin(1, pmax(0, x))
deg2rad <- function(x) x * pi / 180
rad2deg <- function(x) x * 180 / pi

haversine_km <- function(lat1, lon1, lat2, lon2) {
  r <- 6371
  dlat <- deg2rad(lat2 - lat1)
  dlon <- deg2rad(lon2 - lon1)
  a <- sin(dlat / 2) ^ 2 +
    cos(deg2rad(lat1)) * cos(deg2rad(lat2)) * sin(dlon / 2) ^ 2
  2 * r * atan2(sqrt(a), sqrt(1 - a))
}

first_finite <- function(...) {
  vals <- c(...)
  vals <- vals[is.finite(vals)]
  if (!length(vals)) return(NA_real_)
  vals[[1]]
}

bin5 <- function(dd_deg) {
  ifelse(is.na(dd_deg), NA_real_, (round(dd_deg / 5) * 5) %% 360)
}

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
    return(list(
      altitude_rad = rep(NA_real_, n),
      altitude_deg = rep(NA_real_, n),
      azimuth_deg = rep(NA_real_, n)
    ))
  }

  doy <- lubridate::yday(time_local)
  hour_local <- lubridate::hour(time_local) +
    lubridate::minute(time_local) / 60 +
    lubridate::second(time_local) / 3600
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
  azimuth_deg <- (rad2deg(azimuth_rad) + 180) %% 360

  list(
    altitude_rad = altitude_rad,
    altitude_deg = rad2deg(altitude_rad),
    azimuth_deg = azimuth_deg
  )
}

compute_surface_solar_ratio <- function(time_local, lat_deg, lon_deg,
                                        surface_aspect_deg, surface_tilt_deg,
                                        sun_visible,
                                        horiz_floor = 0.15,
                                        max_ratio = 2.6) {
  n <- length(time_local)
  ratio <- ifelse(sun_visible > 0, 1, 0)
  if (n == 0) return(ratio)

  if (!all(is.finite(c(lat_deg, lon_deg, surface_aspect_deg, surface_tilt_deg)))) {
    return(ratio)
  }

  sol <- compute_solar_position(time_local, lat_deg, lon_deg)
  aspect_rad <- deg2rad(surface_aspect_deg %% 360)
  tilt_rad <- deg2rad(pmin(89, pmax(0, surface_tilt_deg)))
  azimuth_rad <- deg2rad(sol$azimuth_deg)

  face_cos <- sin(sol$altitude_rad) * cos(tilt_rad) +
    cos(sol$altitude_rad) * sin(tilt_rad) * cos(azimuth_rad - aspect_rad)
  horiz_cos <- pmax(sin(sol$altitude_rad), horiz_floor)
  raw_ratio <- pmax(0, face_cos) / horiz_cos

  usable <- sun_visible > 0 &
    is.finite(raw_ratio) &
    is.finite(sol$altitude_deg) &
    sol$altitude_deg > 0

  ratio[usable] <- pmin(max_ratio, pmax(1, raw_ratio[usable]))
  ratio
}

season_label_from_year <- function(start_year) {
  sprintf("%02d/%02d", start_year %% 100, (start_year + 1) %% 100)
}

season_plot_date <- function(x) {
  x <- as.Date(x)
  md <- format(x, "%m-%d")
  plot_year <- ifelse(lubridate::month(x) >= 10, 2000, 2001)
  as.Date(sprintf("%04d-%s", plot_year, md))
}

parse_time_utc_geosphere <- function(x) {
  x <- as.character(x)
  x[x %in% c("", "NA", "NaN", "NULL")] <- NA_character_
  has_colon_tz <- grepl("([+-][0-9]{2}):([0-9]{2})$", x)
  x[has_colon_tz] <- paste0(
    substr(x[has_colon_tz], 1, nchar(x[has_colon_tz]) - 3),
    substr(x[has_colon_tz], nchar(x[has_colon_tz]) - 1, nchar(x[has_colon_tz]))
  )
  as.POSIXct(strptime(x, "%Y-%m-%dT%H:%M%z", tz = "UTC"))
}

parse_geosphere_csv <- function(txt) {
  df <- readr::read_csv(
    I(txt),
    show_col_types = FALSE,
    progress = FALSE,
    col_types = readr::cols(time = readr::col_character(), .default = readr::col_guess())
  )

  pick_num <- function(nm) {
    if (nm %in% names(df)) return(to_num(df[[nm]]))
    rep(NA_real_, nrow(df))
  }

  df %>%
    transmute(
      time = with_tz(parse_time_utc_geosphere(time), TZ_LOCAL),
      TL = pick_num("tl"),
      RF = pick_num("rf"),
      FF = pick_num("ff"),
      DD = pick_num("dd"),
      CGLO = pick_num("cglo")
    ) %>%
    filter(!is.na(time))
}

download_geosphere_param_set <- function(station_id, season_start, season_end, params, cache_dir, cache_prefix) {
  param_key <- paste(params, collapse = "_")
  cache_file <- file.path(
    cache_dir,
    sprintf(
      "%s_%s_%s_%s_%s.csv",
      cache_prefix,
      station_id,
      format(season_start, "%Y%m%d"),
      format(season_end, "%Y%m%d"),
      param_key
    )
  )

  if (!file.exists(cache_file) || file.info(cache_file)$size == 0) {
    req <- request("https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-10min") |>
      req_url_query(
        station_ids = station_id,
        parameters = paste(params, collapse = ","),
        start = as.character(season_start),
        end = as.character(season_end + 1),
        output_format = "csv"
      ) |>
      req_user_agent("hist-klima/1.0 (R httr2)") |>
      req_retry(max_tries = 4)

    resp <- req_perform(req)
    if (resp_status(resp) >= 400) {
      stop("GeoSphere request failed for station ", station_id, " (HTTP ", resp_status(resp), ").")
    }

    writeLines(resp_body_string(resp), cache_file, useBytes = TRUE)
  }

  parse_geosphere_csv(readChar(cache_file, file.info(cache_file)$size, useBytes = TRUE))
}

mean_na <- function(x) {
  m <- mean(x, na.rm = TRUE)
  if (is.nan(m)) NA_real_ else m
}

max_na <- function(x) {
  if (all(is.na(x))) return(NA_real_)
  max(x, na.rm = TRUE)
}

score_T_fun_vec <- function(Tv, Topt, Tmin, Tmax, rangeT) {
  s <- 1 - abs(Tv - Topt) / rangeT
  s[Tv <= Tmin | Tv >= Tmax] <- 0
  s[!is.finite(s)] <- NA_real_
  pmax(0, s)
}

assign <- readr::read_csv(path_assign, show_col_types = FALSE, progress = FALSE)
stations_all <- readr::read_csv(path_stations, show_col_types = FALSE, progress = FALSE)
structure_all <- readr::read_csv(path_structure, show_col_types = FALSE, progress = FALSE)
wind_lut <- readr::read_csv(path_wind_lut, show_col_types = FALSE, progress = FALSE)

row_uid <- assign %>% filter(uid == TARGET_UID) %>% slice(1)
if (nrow(row_uid) == 0) stop("UID ", TARGET_UID, " not found in icefalls_nearest_station.csv")

station_id <- as.character(row_uid$station_id)
source <- as.character(row_uid$source)
if (!identical(source, "GeoSphere")) {
  stop("This historical script expects a GeoSphere source for UID ", TARGET_UID, ".")
}

ice_name <- row_uid$icefall_name %||% row_uid$name %||% "Amphitrite"
ice_lat <- to_num(row_uid$ice_lat)
ice_lon <- to_num(row_uid$ice_lon)
ice_alt_m <- to_num(row_uid$icefall_elev_m)
dist_km <- to_num(row_uid$dist_km)
dz_m <- to_num(row_uid$elev_diff_m)
path_barrier_m_uid <- to_num(row_uid$path_barrier_m)
topo_pos_diff_uid <- to_num(row_uid$topo_pos_diff)

st_meta <- stations_all %>% filter(as.character(station_id) == .env$station_id) %>% slice(1)
z_aws <- if (nrow(st_meta) == 1) to_num(st_meta$altitude_m) else NA_real_
station_lat <- first_finite(to_num(row_uid$station_lat), if (nrow(st_meta) == 1) to_num(st_meta$lat) else NA_real_)
station_lon <- first_finite(to_num(row_uid$station_lon), if (nrow(st_meta) == 1) to_num(st_meta$lon) else NA_real_)

structure_uid <- structure_all %>% filter(uid == TARGET_UID) %>% slice(1)
ice_aspect_deg_uid <- if (nrow(structure_uid) == 1) {
  first_finite(
    to_num(structure_uid$preferred_aspect_deg),
    to_num(structure_uid$source_aspect_deg)
  )
} else {
  NA_real_
}
ice_slope_mean_deg_uid <- if (nrow(structure_uid) == 1) to_num(structure_uid$slope_mean_deg) else NA_real_
ice_slope_p90_deg_uid <- if (nrow(structure_uid) == 1) to_num(structure_uid$slope_p90_deg) else NA_real_
ice_tilt_seed_deg_uid <- first_finite(
  if (is.finite(ice_slope_mean_deg_uid) && is.finite(ice_slope_p90_deg_uid)) {
    0.35 * ice_slope_mean_deg_uid + 0.65 * ice_slope_p90_deg_uid
  } else {
    NA_real_
  },
  ice_slope_p90_deg_uid,
  ice_slope_mean_deg_uid,
  65
)
ice_tilt_deg_uid <- pmin(85, pmax(45, ice_tilt_seed_deg_uid))

wind_uid <- wind_lut %>%
  filter(uid == TARGET_UID) %>%
  mutate(dir_deg = as.numeric(dir_deg), wind_vuln_0_9 = as.integer(wind_vuln_0_9)) %>%
  select(dir_deg, wind_vuln_0_9)
if (!nrow(wind_uid)) stop("No wind vulnerability LUT found for UID ", TARGET_UID)

cglo_available_primary <- identical(as.character(st_meta$cglo[[1]]), "TRUE")
solar_station_id <- station_id
solar_station_name <- as.character(st_meta$name[[1]] %||% row_uid$station_name)
solar_station_distance_km <- dist_km
solar_source_note <- "Primary station cglo"

if (!cglo_available_primary) {
  solar_candidates <- stations_all %>%
    filter(
      source == "GeoSphere",
      cglo == TRUE | cglo == "TRUE"
    ) %>%
    mutate(
      lat_num = to_num(lat),
      lon_num = to_num(lon)
    ) %>%
    filter(is.finite(lat_num), is.finite(lon_num)) %>%
    mutate(dist_to_ice_km = haversine_km(ice_lat, ice_lon, lat_num, lon_num)) %>%
    arrange(dist_to_ice_km)

  if (!nrow(solar_candidates)) {
    stop("No GeoSphere fallback station with cglo found.")
  }

  solar_station_id <- as.character(solar_candidates$station_id[[1]])
  solar_station_name <- as.character(solar_candidates$name[[1]])
  solar_station_distance_km <- as.numeric(solar_candidates$dist_to_ice_km[[1]])
  solar_source_note <- "Fallback GeoSphere cglo"
}

ice_params <- list(
  albedo = 0.50,
  solar_boost_strength = 1.35,
  solar_core_boost_strength = 0.55,
  solar_load_cap = 3.0,
  solar_core_cap = 1.8,
  Hmax_m = 0.90,
  H0_m = 0.20,
  Hsat_lambda_m = 0.15,
  core_seed_mm = 4,
  core_seed_boost_mm = 55,
  surface_seed_mm = 16,
  core_share_min = 0.02,
  core_share_min_boost = 0.22,
  core_share_max = 0.10,
  core_share_max_boost = 0.48,
  melt_damping_max = 0.05,
  melt_damping_boost = 0.50,
  melt_damping_scale_m = 0.45,
  core_melt_base = 0.48,
  core_melt_base_drop = 0.32,
  core_melt_warm = 0.30,
  core_melt_warm_drop = 0.12,
  core_melt_sun = 0.24,
  core_melt_sun_drop = 0.10
)

coef <- list(
  lapse_K_per_m = 0.0065,
  growth_mm_per_C_h = 0.50,
  melt_mm_per_C_h = 0.60,
  rad_melt_mm_per_MJ = 0.35,
  k_wind = 0.06,
  k_dry = 0.25,
  wind_cap_ms = 15
)

H_MIN <- 0.07
H_OPT <- 0.45
T_OPT <- -3.5
T_MIN <- -20
T_MAX <- 1.5
RANGE_T <- max(T_OPT - T_MIN, T_MAX - T_OPT)
T3_OPT <- -6
T3_MIN <- -20
T3_MAX <- 0
RANGE_T3 <- max(T3_OPT - T3_MIN, T3_MAX - T3_OPT)
RH_OPT <- 0.70
RH_SIG <- 0.20
CORE_T3_BUFFER_C_PER_M <- 8.0
CORE_CLIMB_BONUS_MULT <- 1.5
CORE_CLIMB_BONUS_DECAY_M <- 0.25
WIN_72H <- as.integer(72 * 60 / MODEL_STEP_MIN)
WIN_24H <- as.integer(24 * 60 / MODEL_STEP_MIN)
WIN_48H <- as.integer(48 * 60 / MODEL_STEP_MIN)
WIN_120H <- as.integer(120 * 60 / MODEL_STEP_MIN)

retention_dz_fac_uid <- clamp01((abs(dz_m) - 150) / 300)
retention_barrier_fac_uid <- clamp01((path_barrier_m_uid - 80) / 160)
retention_topo_fac_uid <- clamp01((0.16 - topo_pos_diff_uid) / 0.10)
retention_fac_uid <- retention_dz_fac_uid * retention_barrier_fac_uid * retention_topo_fac_uid
exposure_barrier_fac_uid <- 1 - clamp01((path_barrier_m_uid - 40) / 120)
exposure_topo_fac_uid <- clamp01((topo_pos_diff_uid - 0.18) / 0.12)
exposure_fac_uid <- exposure_barrier_fac_uid * exposure_topo_fac_uid

run_single_season <- function(start_year) {
  season_start <- as.Date(sprintf("%d-10-01", start_year))
  season_end_target <- as.Date(sprintf("%d-04-30", start_year + 1))
  season_end <- min(season_end_target, Sys.Date())
  if (season_end <= season_start) return(NULL)

  message("Building season ", season_label_from_year(start_year), " ...")

  wx_core <- download_geosphere_param_set(
    station_id = station_id,
    season_start = season_start,
    season_end = season_end,
    params = c("tl", "rf", "ff", "dd"),
    cache_dir = dir_cache,
    cache_prefix = "wx"
  )

  solar10 <- download_geosphere_param_set(
    station_id = solar_station_id,
    season_start = season_start,
    season_end = season_end,
    params = c("cglo"),
    cache_dir = dir_cache,
    cache_prefix = "solar"
  ) %>%
    select(time, CGLO)

  wx10 <- wx_core %>%
    select(time, TL, RF, FF, DD) %>%
    left_join(solar10, by = "time")

  if (!nrow(wx10)) return(NULL)

  step_str <- paste0(MODEL_STEP_MIN, " mins")
  wx <- wx10 %>%
    mutate(time = floor_date(time, unit = step_str)) %>%
    group_by(time) %>%
    summarise(
      TL = mean_na(TL),
      RF = mean_na(RF),
      FF = mean_na(FF),
      DD = mean_na(DD),
      CGLO = mean_na(CGLO),
      .groups = "drop"
    ) %>%
    tidyr::complete(time = seq(min(time), max(time), by = step_str)) %>%
    arrange(time) %>%
    mutate(
      TL = fill1(TL),
      RF = fill1(RF),
      FF = fill1(FF),
      DD = fill1(DD),
      CGLO = fill1(CGLO)
    ) %>%
    filter(
      time >= as.POSIXct(season_start, tz = TZ_LOCAL),
      time < as.POSIXct(season_end + 1, tz = TZ_LOCAL)
    )

  if (!nrow(wx)) return(NULL)

  wx <- wx %>%
    mutate(
      is_forecast = FALSE,
      inv_active = FALSE,
      inv_score_C = 0,
      inv_class = "none",
      inv_grad_max_C_per_100m = NA_real_,
      grad01_K_per_m = NA_real_,
      grad12_K_per_m = NA_real_,
      grad02_K_per_m = NA_real_,
      date = as.Date(time),
      dd_bin = bin5(DD)
    ) %>%
    left_join(wind_uid, by = c("dd_bin" = "dir_deg")) %>%
    mutate(
      wind_vuln_0_9 = ifelse(is.na(wind_vuln_0_9), 9L, wind_vuln_0_9),
      wind_vuln = pmin(1, pmax(0, wind_vuln_0_9 / 9))
    )

  sol <- compute_solar_position(wx$time, ice_lat, ice_lon)
  sun_visible <- as.numeric(sol$altitude_deg > 0)
  wx$solar_incidence_ratio <- compute_surface_solar_ratio(
    time_local = wx$time,
    lat_deg = ice_lat,
    lon_deg = ice_lon,
    surface_aspect_deg = ice_aspect_deg_uid,
    surface_tilt_deg = ice_tilt_deg_uid,
    sun_visible = sun_visible
  )
  wx$topo_sun_fac <- sun_visible

  wx <- wx %>%
    mutate(
      dz_raw = dz_m,
      use_prof = FALSE,
      z_target_m = z_aws + dz_m,
      dT_prof = NA_real_,
      TLz_raw = TL - coef$lapse_K_per_m * dz_raw,
      TLz = TLz_raw,
      FDH = pmax(0, -TLz),
      PDH = pmax(0, TLz),
      solar_load_fac = if_else(
        topo_sun_fac > 0,
        pmin(
          ice_params$solar_load_cap,
          1 + ice_params$solar_boost_strength * pmax(0, solar_incidence_ratio - 1)
        ),
        0
      ),
      solar_core_fac = if_else(
        topo_sun_fac > 0,
        pmin(
          ice_params$solar_core_cap,
          1 + ice_params$solar_core_boost_strength * pmax(0, solar_incidence_ratio - 1)
        ),
        0
      ),
      SW_MJ_step = CGLO * W2MJ_STEP * solar_load_fac * (1 - ice_params$albedo),
      FF_eff = pmin(coef$wind_cap_ms, pmax(0, FF)),
      wind_fac = 1 + coef$k_wind * FF_eff * wind_vuln,
      dry_fac = 1 + coef$k_dry * pmax(0, 1 - RF / 100),
      base_growth_mm_step = coef$growth_mm_per_C_h * FDH * DT_H * wind_fac * dry_fac,
      base_melt_mm_step = coef$melt_mm_per_C_h * PDH * DT_H * wind_fac +
        coef$rad_melt_mm_per_MJ * SW_MJ_step,
      TLz_72h_step = zoo::rollapplyr(
        TLz,
        width = WIN_72H,
        FUN = function(x) mean(x, na.rm = TRUE),
        fill = NA_real_,
        partial = TRUE
      )
    )

  surface_mm <- numeric(nrow(wx))
  core_mm <- numeric(nrow(wx))
  core_reserve_mm <- numeric(nrow(wx))
  surface_mm[1] <- ice_params$surface_seed_mm
  core_mm[1] <- ice_params$core_seed_mm + ice_params$core_seed_boost_mm * retention_fac_uid

  for (i in seq.int(2, nrow(wx))) {
    Hprev_m <- (surface_mm[i - 1] + core_mm[i - 1]) / 1000
    iso <- exp(-Hprev_m / ice_params$H0_m)

    gap_m <- pmax(0, ice_params$Hmax_m - Hprev_m)
    cap_exp <- (1 - exp(-gap_m / ice_params$Hsat_lambda_m)) /
      (1 - exp(-ice_params$Hmax_m / ice_params$Hsat_lambda_m))

    growth_total <- wx$base_growth_mm_step[i] * iso * cap_exp

    core_share_min_eff <- ice_params$core_share_min +
      ice_params$core_share_min_boost * retention_fac_uid
    core_share_max_eff <- ice_params$core_share_max +
      ice_params$core_share_max_boost * retention_fac_uid
    core_share <- core_share_min_eff +
      0.14 * pmin(1, wx$FDH[i] / 4) +
      0.12 * pmin(1, Hprev_m / 0.30)
    core_share <- pmin(core_share_max_eff, pmax(core_share_min_eff, core_share))

    growth_core <- growth_total * core_share
    growth_surface <- growth_total - growth_core

    surface_pre_melt <- surface_mm[i - 1] + growth_surface
    core_pre_melt <- core_mm[i - 1] + growth_core

    melt_damping_eff <- ice_params$melt_damping_max +
      ice_params$melt_damping_boost * retention_fac_uid
    melt_scale <- 1 - melt_damping_eff * pmin(1, Hprev_m / ice_params$melt_damping_scale_m)
    melt_total <- wx$base_melt_mm_step[i] * melt_scale
    spring_exposure_fac <- clamp01((wx$TLz_72h_step[i] + 5) / 5)
    melt_total <- melt_total * (1 + 0.8 * exposure_fac_uid * spring_exposure_fac)

    melt_surface <- min(surface_pre_melt, melt_total)
    melt_left <- pmax(0, melt_total - melt_surface)

    core_melt_fac <- (ice_params$core_melt_base - ice_params$core_melt_base_drop * retention_fac_uid) +
      (ice_params$core_melt_warm - ice_params$core_melt_warm_drop * retention_fac_uid) * pmin(1, wx$PDH[i] / 4) +
      (ice_params$core_melt_sun - ice_params$core_melt_sun_drop * retention_fac_uid) * wx$solar_core_fac[i]
    core_melt_fac <- pmin(0.85, pmax(0.20, core_melt_fac))
    melt_core <- min(core_pre_melt, melt_left * core_melt_fac)

    reserve_gain_mm <- 0.30 * growth_core * retention_fac_uid
    reserve_loss_mm <- (0.08 * wx$PDH[i] * DT_H + 0.05 * wx$solar_core_fac[i] +
      0.02 * pmax(0, wx$TLz_72h_step[i] + 2)) * retention_fac_uid
    reserve_cap_mm <- 0.28 * core_pre_melt
    core_reserve_mm[i] <- min(
      reserve_cap_mm,
      max(0, core_reserve_mm[i - 1] + reserve_gain_mm - reserve_loss_mm)
    )

    surface_mm[i] <- max(0, surface_pre_melt - melt_surface)
    core_mm[i] <- max(core_reserve_mm[i], core_pre_melt - melt_core)
  }

  mod <- wx %>%
    mutate(
      thickness_m = (surface_mm + core_mm) / 1000,
      surface_ice_m = surface_mm / 1000,
      core_ice_m = core_mm / 1000,
      thaw_flag = TLz > 0,
      thaw_transition = c(0, abs(diff(as.integer(thaw_flag)))),
      TLz_72h = zoo::rollapplyr(
        TLz,
        width = WIN_72H,
        FUN = function(x) mean(x, na.rm = TRUE),
        fill = NA_real_,
        partial = TRUE
      ),
      PDH_24h = zoo::rollapplyr(
        PDH * DT_H,
        width = WIN_24H,
        FUN = function(x) sum(x, na.rm = TRUE),
        fill = NA_real_,
        partial = TRUE
      ),
      PDH_72h = zoo::rollapplyr(
        PDH * DT_H,
        width = WIN_72H,
        FUN = function(x) sum(x, na.rm = TRUE),
        fill = NA_real_,
        partial = TRUE
      ),
      SW_48h_MJ = zoo::rollapplyr(
        SW_MJ_step,
        width = WIN_48H,
        FUN = function(x) sum(x, na.rm = TRUE),
        fill = NA_real_,
        partial = TRUE
      ),
      thaw_cycles_120h = zoo::rollapplyr(
        thaw_transition,
        width = WIN_120H,
        FUN = function(x) sum(x, na.rm = TRUE) / 2,
        fill = NA_real_,
        partial = TRUE
      ),
      TLz_72h_eff = TLz_72h - CORE_T3_BUFFER_C_PER_M * core_ice_m,
      core_climb_bonus_m = core_ice_m * CORE_CLIMB_BONUS_MULT *
        clamp01(1 - thickness_m / CORE_CLIMB_BONUS_DECAY_M),
      climb_thickness_m = thickness_m + core_climb_bonus_m,
      score_h = pmin(1, pmax(0, (climb_thickness_m - H_MIN) / (H_OPT - H_MIN))),
      score_T = score_T_fun_vec(TLz, T_OPT, T_MIN, T_MAX, RANGE_T),
      score_T3 = score_T_fun_vec(TLz_72h_eff, T3_OPT, T3_MIN, T3_MAX, RANGE_T3),
      score_RH = exp(-((RF / 100) - RH_OPT)^2 / (2 * RH_SIG^2)),
      score_rot_warm = exp(-0.14 * coalesce(PDH_24h, 0) - 0.05 * coalesce(PDH_72h, 0)),
      score_rot_sun = exp(-0.06 * coalesce(SW_48h_MJ, 0)),
      score_rot_cycle = exp(-0.30 * coalesce(thaw_cycles_120h, 0)),
      score_structure_raw = score_rot_warm * score_rot_sun * score_rot_cycle,
      core_structure_floor = 0.10 + 0.45 * clamp01(core_ice_m / 0.12),
      score_structure = pmax(score_structure_raw, core_structure_floor),
      climbability = score_h * score_T * score_T3 * score_RH * score_structure,
      core_climb_floor = 0.22 * retention_fac_uid * clamp01(core_ice_m / 0.08) *
        exp(-0.08 * coalesce(PDH_24h, 0) - 0.03 * coalesce(PDH_72h, 0) - 0.05 * coalesce(SW_48h_MJ, 0)),
      climbability = pmax(climbability, core_climb_floor),
      climbability = ifelse(climb_thickness_m < H_MIN, NA_real_, climbability),
      climbability = pmin(1, pmax(0, climbability)),
      season = season_label_from_year(start_year),
      season_plot_date = season_plot_date(date)
    )

  mod %>%
    group_by(season, date, season_plot_date) %>%
    summarise(
      thickness_m = max_na(thickness_m),
      climbability = max_na(climbability),
      TLz = mean_na(TLz),
      RF = mean_na(RF),
      .groups = "drop"
    )
}

last_start_year <- if (lubridate::month(Sys.Date()) >= 10) {
  lubridate::year(Sys.Date())
} else {
  lubridate::year(Sys.Date()) - 1
}

season_years <- seq.int(FIRST_SEASON_START_YEAR, last_start_year)
season_daily <- bind_rows(lapply(season_years, run_single_season))

if (!nrow(season_daily)) {
  stop("No historical season data could be built for UID ", TARGET_UID)
}

season_levels <- season_years |> vapply(season_label_from_year, character(1))
season_daily <- season_daily %>%
  mutate(season = factor(season, levels = season_levels, ordered = TRUE))

season_summary <- season_daily %>%
  group_by(season) %>%
  summarise(
    mean_climbability = mean(climbability, na.rm = TRUE),
    max_climbability = max_na(climbability),
    mean_thickness = mean(thickness_m, na.rm = TRUE),
    valid_climb_days = sum(!is.na(climbability)),
    .groups = "drop"
  ) %>%
  arrange(desc(mean_climbability))

season_summary_valid <- season_summary %>%
  filter(is.finite(mean_climbability), valid_climb_days > 0)

best_season <- as.character(season_summary_valid$season[[1]])
worst_season <- as.character(season_summary_valid$season[[nrow(season_summary_valid)]])

plot_data <- bind_rows(
  season_daily %>%
    transmute(season, season_plot_date, metric = "Ice thickness (m)", value = thickness_m),
  season_daily %>%
    transmute(season, season_plot_date, metric = "Climbability (0-1)", value = climbability)
)

pal <- grDevices::colorRampPalette(c("#c7e9f1", "#73b3d8", "#2b7bba", "#08306b"))(length(season_levels))
names(pal) <- season_levels

plot_title <- paste0("Historical seasons - ", ice_name, " (UID ", TARGET_UID, ")")
plot_subtitle <- paste(
  c(
    paste0("Station ", station_id, " (GeoSphere)"),
    paste0("Solar source ", solar_station_name, " (", solar_station_id, ")"),
    paste0("dist ", round(dist_km, 2), " km"),
    paste0("dz ", round(dz_m, 0), " m"),
    paste0("Elevation ", round(ice_alt_m, 0), " m"),
    paste0("Seasons from ", season_label_from_year(FIRST_SEASON_START_YEAR), " onward")
  ),
  collapse = " | "
)

plt <- ggplot(plot_data, aes(season_plot_date, value, color = season, group = season)) +
  geom_line(linewidth = 0.8, alpha = 0.95) +
  facet_wrap(~ metric, ncol = 1, scales = "free_y") +
  scale_color_manual(values = pal, drop = FALSE) +
  scale_x_date(
    breaks = seq(as.Date("2000-10-01"), as.Date("2001-04-01"), by = "1 month"),
    date_labels = "%b"
  ) +
  labs(
    title = plot_title,
    subtitle = plot_subtitle,
    x = "Season month",
    y = NULL,
    color = "Season"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    legend.position = "right",
    panel.grid.minor = element_blank(),
    strip.text = element_text(face = "bold"),
    plot.title = element_text(face = "bold"),
    axis.text.x = element_text(size = 9)
  )

path_plot <- file.path(dir_output, "uid054_hist_klima.png")
path_daily <- file.path(dir_output, "uid054_hist_klima_daily.csv")
path_summary <- file.path(dir_output, "uid054_hist_klima_summary.csv")
path_compare <- file.path(dir_output, "uid054_best_vs_worst.png")

readr::write_csv(season_daily, path_daily)
readr::write_csv(season_summary, path_summary)
ggsave(path_plot, plot = plt, width = 13, height = 9, units = "in", dpi = 200, bg = "white")

compare_data <- plot_data %>%
  filter(as.character(season) %in% c(best_season, worst_season)) %>%
  mutate(
    season_flag = factor(
      ifelse(as.character(season) == best_season, paste0("Best: ", best_season), paste0("Worst: ", worst_season)),
      levels = c(paste0("Best: ", best_season), paste0("Worst: ", worst_season))
    )
  )

compare_subtitle <- paste(
  c(
    paste0("Best season by mean climbability: ", best_season),
    paste0("Worst season by mean climbability: ", worst_season),
    paste0("Station ", station_id, " (GeoSphere)"),
    paste0("Solar source ", solar_station_name, " (", solar_station_id, ")")
  ),
  collapse = " | "
)

plt_compare <- ggplot(compare_data, aes(season_plot_date, value, color = season_flag, group = season_flag)) +
  geom_line(linewidth = 1) +
  facet_wrap(~ metric, ncol = 1, scales = "free_y") +
  scale_color_manual(
    values = stats::setNames(
      c("#1b9e77", "#d95f02"),
      c(paste0("Best: ", best_season), paste0("Worst: ", worst_season))
    ),
    drop = FALSE
  ) +
  scale_x_date(
    breaks = seq(as.Date("2000-10-01"), as.Date("2001-04-01"), by = "1 month"),
    date_labels = "%b"
  ) +
  labs(
    title = paste0("Best vs worst season - ", ice_name, " (UID ", TARGET_UID, ")"),
    subtitle = compare_subtitle,
    x = "Season month",
    y = NULL,
    color = NULL
  ) +
  theme_minimal(base_size = 12) +
  theme(
    legend.position = "top",
    panel.grid.minor = element_blank(),
    strip.text = element_text(face = "bold"),
    plot.title = element_text(face = "bold"),
    axis.text.x = element_text(size = 9)
  )

ggsave(path_compare, plot = plt_compare, width = 12, height = 8, units = "in", dpi = 200, bg = "white")

message("Daily summary written to: ", path_daily)
message("Season summary written to: ", path_summary)
message("Plot written to: ", path_plot)
message("Best season: ", best_season, " | Worst season: ", worst_season)
message("Comparison plot written to: ", path_compare)
message("Solar source used: ", solar_station_name, " (", solar_station_id, ") | ", solar_source_note)
