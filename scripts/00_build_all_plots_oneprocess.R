# =====================================================================
# Build ALL icefall model runs + plots in ONE process (fast)
#
# POINT-BASED (SMALL) DOWNLOAD STRATEGY
#   - Station TL/RF (10-min): downloaded ONCE per (station_id, source) -> cached RDS
#   - INCA (1km, hourly UU,VV,GL): point-timeseries -> cached CSV (dedup by INCA key)
#   - NWP  (2.5km, hourly t2m,rh2m,u10m,v10m,grad): point-forecast -> cached CSV (dedup by NWP key)
#
# Debugging:
#   Sys.setenv(LOG_LEVEL='2', ONLY_UID='87')
#   source('scripts/00_build_all_plots_oneprocess.R')
#
# Notes:
#   - Tirol-wide INCA/NWP NetCDF for a full season is too large (API/file size limits).
#   - Dedup works because many icefalls fall into the same model grid cell.
#   - GitHub Actions: to persist cache between runs, add actions/cache for:
#       data/inca_nordtirol/
#       data/nwp_2500m_forecast/
#       data/_cache_geosphere/
# =====================================================================

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(readr)
  library(zoo)
  library(ggplot2)
  library(patchwork)
  library(parallel)
})

TZ_LOCAL <- 'Europe/Vienna'
MODEL_STEP_MIN <- 10
DT_H   <- MODEL_STEP_MIN / 60
DT_SEC <- MODEL_STEP_MIN * 60
W2MJ_STEP <- DT_SEC / 1e6  # W/m² -> MJ/m² per step
FORECAST_HOURS <- 60

# ----------------------------
# Logging / debugging
# ----------------------------
# LOG_LEVEL:
#   0 = quiet
#   1 = high-level (default)
#   2 = + summaries + per-UID metadata
#   3 = + per-request API details (very chatty)
LOG_LEVEL <- suppressWarnings(as.integer(Sys.getenv('LOG_LEVEL', '1')))
if (!is.finite(LOG_LEVEL)) LOG_LEVEL <- 1
ONLY_UID <- Sys.getenv('ONLY_UID', '')

.ts_now <- function() format(with_tz(Sys.time(), TZ_LOCAL), '%Y-%m-%d %H:%M:%S')
log_msg <- function(level = 1, ...) {
  if (LOG_LEVEL >= level) message(sprintf('[%s] %s', .ts_now(), paste0(..., collapse = '')))
}
log_header <- function(title, level = 1) {
  if (LOG_LEVEL >= level) {
    message("
==============================")
    message(title)
    message('==============================')
  }
}

# ----------------------------
# Dedup settings
# ----------------------------
# Higher decimals => fewer merges (safer). Lower decimals => more reuse (faster).
INCA_KEY_DECIMALS <- suppressWarnings(as.integer(Sys.getenv('INCA_KEY_DECIMALS', '3')))
NWP_KEY_DECIMALS  <- suppressWarnings(as.integer(Sys.getenv('NWP_KEY_DECIMALS',  '3')))
if (!is.finite(INCA_KEY_DECIMALS) || INCA_KEY_DECIMALS < 2) INCA_KEY_DECIMALS <- 3
if (!is.finite(NWP_KEY_DECIMALS)  || NWP_KEY_DECIMALS  < 2) NWP_KEY_DECIMALS  <- 3

# ----------------------------
# Concurrency
# ----------------------------
N_WORKERS <- suppressWarnings(as.integer(Sys.getenv('N_WORKERS', '4')))
if (!is.finite(N_WORKERS) || N_WORKERS < 1) N_WORKERS <- 1
N_WORKERS <- min(N_WORKERS, max(1, parallel::detectCores(logical = TRUE) - 1))

# ----------------------------
# Paths
# ----------------------------
PATH_ASSIGN   <- 'data/AWS/icefalls_nearest_station.csv'
PATH_STATIONS <- 'data/AWS/stations_all.csv'
PATH_SUN      <- 'data/Koordinaten_Wasserfaelle/icefalls_sun_horizon.csv'
PATH_WINDLUT  <- 'data/Wind/wind_vulnerability_5deg.csv'

DIR_CACHE   <- 'data/_cache_geosphere'
DIR_STATION <- file.path(DIR_CACHE, 'station')
PATH_INCA_DIR <- 'data/inca_nordtirol'
PATH_NWP_DIR  <- 'data/nwp_2500m_forecast'
DIR_MODEL <- 'data/ModelRuns'
DIR_PLOTS <- 'site/plots'

safe_mkdir <- function(p) dir.create(p, recursive = TRUE, showWarnings = FALSE)
for (d in c(DIR_CACHE, DIR_STATION, PATH_INCA_DIR, PATH_NWP_DIR, DIR_MODEL, DIR_PLOTS)) safe_mkdir(d)

# ----------------------------
# Helpers
# ----------------------------
season_start_oct <- function(d) {
  d <- as.Date(d)
  y <- year(d); m <- month(d)
  as.Date(sprintf('%d-10-01', ifelse(m >= 10, y, y - 1)))
}

season_label <- function(date) {
  y <- year(date); m <- month(date)
  ifelse(m >= 10, sprintf('%d_%d', y, y + 1), sprintf('%d_%d', y - 1, y))
}

to_num <- function(x) {
  if (is.numeric(x)) return(x)
  x <- as.character(x)
  x <- gsub(',', '.', x, fixed = TRUE)
  suppressWarnings(as.numeric(x))
}

bin5 <- function(dd_deg) {
  ifelse(is.na(dd_deg), NA_real_, (round(dd_deg / 5) * 5) %% 360)
}

fill1 <- function(x) {
  x <- zoo::na.approx(x, na.rm = FALSE)
  x <- zoo::na.locf(x, na.rm = FALSE)
  x <- zoo::na.locf(x, na.rm = FALSE, fromLast = TRUE)
  x
}

read_geosphere_csv <- function(path) {
  x <- tryCatch(readr::read_delim(path, delim = ',', show_col_types = FALSE, progress = FALSE), error = function(e) NULL)
  if (!is.null(x) && ncol(x) > 1) return(x)
  x <- tryCatch(readr::read_delim(path, delim = ';', show_col_types = FALSE, progress = FALSE), error = function(e) NULL)
  if (!is.null(x) && ncol(x) > 1) return(x)
  readr::read_csv(path, show_col_types = FALSE, progress = FALSE)
}

parse_time_any <- function(x, tz = TZ_LOCAL) {
  if (inherits(x, 'POSIXct')) return(with_tz(x, tz))
  x <- as.character(x)
  x[x %in% c('', 'NA', 'NaN', 'NULL')] <- NA_character_
  out <- suppressWarnings(lubridate::ymd_hms(x, tz = tz))
  if (all(is.na(out))) out <- suppressWarnings(lubridate::ymd_hm(x, tz = tz))
  out
}

parse_time_any_utc <- function(x) {
  x <- as.character(x)
  x[x %in% c('', 'NA', 'NaN', 'NULL')] <- NA_character_
  t <- suppressWarnings(lubridate::ymd_hms(x, tz = 'UTC'))
  if (all(is.na(t))) t <- suppressWarnings(lubridate::ymd_hm(x, tz = 'UTC'))
  if (all(is.na(t))) t <- suppressWarnings(lubridate::parse_date_time(
    x, orders = c('Ymd HMS', 'Ymd HM', 'Y-m-d"T"H:M:S', 'Y-m-d"T"H:M'), tz = 'UTC'
  ))
  t
}

# Vector-safe coordinate key (dedup)
coord_key <- function(prefix, lat, lon, decimals = 3) {
  lat <- to_num(lat)
  lon <- to_num(lon)
  ok <- is.finite(lat) & is.finite(lon)
  out <- rep(NA_character_, length(lat))
  if (!any(ok)) return(out)
  fmt <- paste0('%.' , decimals, 'f')
  out[ok] <- paste0(prefix, '_', sprintf(fmt, round(lat[ok], decimals)), '_', sprintf(fmt, round(lon[ok], decimals)))
  out <- gsub('[^A-Za-z0-9_-]', '_', out)
  out
}

# ----------------------------
# Date window
# ----------------------------
NOW_LOCAL <- with_tz(Sys.time(), TZ_LOCAL)
END_DATE  <- as.Date(NOW_LOCAL)
START_DATE <- season_start_oct(END_DATE)
END_DATE_EXT <- as.Date(NOW_LOCAL + hours(FORECAST_HOURS))

# Use "now" rounded down to 10-min as historic end.
HIST_END_LOCAL <- floor_date(NOW_LOCAL, unit = paste0(MODEL_STEP_MIN, ' minutes'))
FC_END_LOCAL   <- HIST_END_LOCAL + hours(FORECAST_HOURS)

# ----------------------------
# Read inputs
# ----------------------------
stopifnot(file.exists(PATH_ASSIGN), file.exists(PATH_STATIONS), file.exists(PATH_SUN), file.exists(PATH_WINDLUT))

assign <- readr::read_csv(PATH_ASSIGN, show_col_types = FALSE, progress = FALSE)
if (!('uid' %in% names(assign))) stop('Spalte uid fehlt in ', PATH_ASSIGN)

# keep only needed cols (but tolerate extra)
need_cols <- c('uid','station_id','source','dist_km','elev_diff_m','ice_lon','ice_lat','name','ice_name','icefall_name','icefall_elev_m','icefall_height_m')
# do not drop columns aggressively because your file sometimes changes; just ensure required exist

assign <- assign %>%
  mutate(
    uid = suppressWarnings(as.integer(uid)),
    ice_lon = to_num(ice_lon),
    ice_lat = to_num(ice_lat)
  )

stations_all <- readr::read_csv(
  PATH_STATIONS, show_col_types = FALSE, progress = FALSE,
  col_select = dplyr::any_of(c('station_id','altitude_m'))
)

sun_all <- readr::read_csv(
  PATH_SUN, show_col_types = FALSE, progress = FALSE,
  col_select = dplyr::any_of(c('uid','name','date','sunrise_topo','sunset_topo','sun_hours_topo'))
)

wind_lut <- readr::read_csv(
  PATH_WINDLUT, show_col_types = FALSE, progress = FALSE,
  col_select = dplyr::any_of(c('uid','dir_deg','wind_vuln_0_9'))
)

# Normalize / coerce (robust against BOM, stray spaces, type drift)
names(wind_lut) <- trimws(names(wind_lut))
wind_lut <- wind_lut %>%
  mutate(
    uid = suppressWarnings(as.integer(uid)),
    dir_deg = to_num(dir_deg),
    wind_vuln_0_9 = suppressWarnings(as.integer(wind_vuln_0_9))
  )

# Robust column normalization (handles BOM/whitespace/renames)
names(wind_lut) <- trimws(names(wind_lut))
names(wind_lut) <- sub('^\ufeff', '', names(wind_lut))  # UTF-8 BOM
names(wind_lut) <- sub('^ï»¿', '', names(wind_lut))       # BOM rendered as text

if (!'uid' %in% names(wind_lut)) {
  alt <- names(wind_lut)[grepl('uid', names(wind_lut), ignore.case = TRUE)]
  if (length(alt) >= 1) wind_lut <- wind_lut %>% rename(uid = all_of(alt[1]))
}
if (!'dir_deg' %in% names(wind_lut)) {
  alt <- names(wind_lut)[grepl('dir', names(wind_lut), ignore.case = TRUE)]
  if (length(alt) >= 1) wind_lut <- wind_lut %>% rename(dir_deg = all_of(alt[1]))
}
if (!'wind_vuln_0_9' %in% names(wind_lut)) {
  alt <- names(wind_lut)[grepl('vuln', names(wind_lut), ignore.case = TRUE)]
  if (length(alt) >= 1) wind_lut <- wind_lut %>% rename(wind_vuln_0_9 = all_of(alt[1]))
}

wind_lut <- wind_lut %>%
  mutate(
    uid = suppressWarnings(as.integer(.data$uid)),
    dir_deg = to_num(.data$dir_deg),
    wind_vuln_0_9 = suppressWarnings(as.integer(.data$wind_vuln_0_9))
  )

uids <- sort(unique(assign$uid[is.finite(assign$uid)]))
if (length(uids) == 0) stop('Keine UIDs gefunden.')

if (nzchar(ONLY_UID)) {
  keep <- suppressWarnings(as.integer(trimws(unlist(strsplit(ONLY_UID, ',')))))
  keep <- keep[is.finite(keep)]
  if (length(keep) > 0) {
    uids <- intersect(uids, keep)
    log_msg(1, 'ONLY_UID active -> running: ', paste(uids, collapse = ', '))
  }
}

# IMPORTANT: from here on, restrict ALL downstream downloads/builds to the selected UIDs
assign_run <- assign %>% filter(.data$uid %in% uids)
if (nrow(assign_run) == 0) stop('ONLY_UID filter resulted in 0 rows. Check UID(s).')

# Optional: reduce sun/wind tables to run-scope (faster joins)
sun_all_run  <- sun_all  %>% filter(.data$uid %in% uids)
wind_lut_run <- wind_lut %>% filter(.data$uid %in% uids)

log_header('RUN SUMMARY', level = 1)
log_msg(1, 'Season start: ', START_DATE, ' | End date: ', END_DATE)
log_msg(1, 'Historic end: ', format(HIST_END_LOCAL, '%Y-%m-%d %H:%M'), ' | Forecast end: ', format(FC_END_LOCAL, '%Y-%m-%d %H:%M'))
log_msg(1, 'UIDs: ', length(uids), ' | workers: ', N_WORKERS)
log_msg(1, 'INCA dedup decimals: ', INCA_KEY_DECIMALS, ' | NWP dedup decimals: ', NWP_KEY_DECIMALS)

# =====================================================================
# Station TL/RF (GeoSphere JSON OR LWD CSV) + cache
# =====================================================================
parse_dt_any <- function(x, tz = TZ_LOCAL) {
  x <- as.character(x)
  x[x %in% c('', 'NA', 'NaN', 'NULL')] <- NA_character_
  out <- suppressWarnings(lubridate::dmy_hms(x, tz = tz))
  if (all(is.na(out))) out <- suppressWarnings(lubridate::dmy_hm(x, tz = tz))
  if (all(is.na(out))) out <- suppressWarnings(lubridate::ymd_hms(x, tz = tz))
  if (all(is.na(out))) out <- suppressWarnings(lubridate::ymd_hm(x, tz = tz))
  out
}

read_lwd_param <- function(station_code, param, season) {
  url <- sprintf('https://wiski.tirol.gv.at/lawine/produkte/ogd/%s/%s_%s_%s.csv', station_code, station_code, param, season)
  resp <- tryCatch(request(url) |> req_user_agent('icefall-model/1.0 (R httr2)') |> req_perform(), error = function(e) NULL)
  if (is.null(resp) || resp_status(resp) != 200) return(NULL)
  raw <- tryCatch(resp_body_raw(resp), error = function(e) NULL)
  if (is.null(raw) || length(raw) == 0) return(NULL)
  txt0 <- tryCatch(rawToChar(raw), error = function(e) NA_character_)
  if (is.na(txt0)) return(NULL)
  txt <- iconv(txt0, from = '', to = 'UTF-8', sub = 'byte')
  
  tmp <- tryCatch(readr::read_delim(file = I(txt), delim = ';', show_col_types = FALSE, progress = FALSE), error = function(e) NULL)
  if (is.null(tmp) || nrow(tmp) == 0) return(NULL)
  
  nms <- names(tmp)
  nms_low <- tolower(iconv(nms, from = '', to = 'ASCII//TRANSLIT', sub = ''))
  dt_i <- which(grepl('datetime|date_time|zeitstempel|timestamp|datumzeit|datum_zeit', nms_low))
  dt_col <- if (length(dt_i)) nms[dt_i[1]] else NA_character_
  
  if (!is.na(dt_col)) {
    t <- parse_dt_any(tmp[[dt_col]], tz = TZ_LOCAL)
    val_col <- setdiff(nms, dt_col)[1]
    return(tibble(timestamp = t, value = to_num(tmp[[val_col]])) %>% filter(!is.na(timestamp)))
  }
  
  if (ncol(tmp) >= 2) {
    t <- parse_dt_any(tmp[[1]], tz = TZ_LOCAL)
    return(tibble(timestamp = t, value = to_num(tmp[[2]])) %>% filter(!is.na(timestamp)))
  }
  NULL
}

get_lwd_station_tlrf <- function(start_date, end_date, station_code) {
  seasons <- unique(season_label(seq(as.Date(start_date), as.Date(end_date), by = 'day')))
  tl <- bind_rows(lapply(seasons, function(seas) read_lwd_param(station_code, 'LT', seas))) %>% mutate(param = 'TL')
  rf <- bind_rows(lapply(seasons, function(seas) read_lwd_param(station_code, 'LF', seas))) %>% mutate(param = 'RF')
  long <- bind_rows(tl, rf)
  if (nrow(long) == 0) stop('Keine LWD Daten für Station ', station_code)
  
  wide <- long %>%
    select(timestamp, param, value) %>%
    pivot_wider(names_from = param, values_from = value) %>%
    arrange(timestamp) %>%
    filter(timestamp >= as.POSIXct(start_date, tz = TZ_LOCAL), timestamp < as.POSIXct(end_date + 1, tz = TZ_LOCAL))
  
  for (cc in c('TL','RF')) if (!cc %in% names(wide)) wide[[cc]] <- NA_real_
  wide
}

get_geosphere_station_tlrf <- function(start_date, end_date, station_id) {
  base_url <- 'https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-10min'
  start_q <- sprintf('%sT00:00', as.character(as.Date(start_date)))
  end_q   <- sprintf('%sT23:50', as.character(as.Date(end_date)))
  
  param_tries <- c('tl,rf', 'TL,RF')
  last_msg <- NULL
  
  for (p in param_tries) {
    if (LOG_LEVEL >= 3) log_msg(3, 'Station request: station_ids=', station_id, ' | params=', p, ' | start=', start_q, ' | end=', end_q)
    
    resp <- request(base_url) |>
      req_url_query(station_ids = as.character(station_id), parameters = p, start = start_q, end = end_q) |>
      req_user_agent('icefall-model/1.0 (R httr2)') |>
      req_retry(max_tries = 3) |>
      req_error(is_error = function(r) FALSE) |>
      req_perform()
    
    st <- resp_status(resp)
    if (st < 400) {
      dat <- jsonlite::fromJSON(resp_body_string(resp), simplifyVector = FALSE)
      ts_raw <- dat[['timestamps']]
      time_utc <- suppressWarnings(lubridate::ymd_hms(ts_raw, tz = 'UTC'))
      if (all(is.na(time_utc))) {
        ts_fix <- ts_raw
        has_colon_tz <- grepl('([+-][0-9]{2}):([0-9]{2})$', ts_fix)
        ts_fix[has_colon_tz] <- paste0(
          substr(ts_fix[has_colon_tz], 1, nchar(ts_fix[has_colon_tz]) - 3),
          substr(ts_fix[has_colon_tz], nchar(ts_fix[has_colon_tz]) - 1, nchar(ts_fix[has_colon_tz]))
        )
        time_utc <- as.POSIXct(strptime(ts_fix, '%Y-%m-%dT%H:%M%z', tz = 'UTC'))
      }
      
      feat   <- dat[['features']][[1]]
      params <- feat[['properties']][['parameters']]
      
      pull_anycase <- function(name) {
        cand <- c(name, tolower(name), toupper(name))
        for (nm in cand) {
          p0 <- params[[nm]]
          if (!is.null(p0) && !is.null(p0[['data']])) {
            lst <- p0[['data']]
            out <- rep(NA_real_, length(time_utc))
            m <- min(length(lst), length(time_utc))
            out[seq_len(m)] <- vapply(lst[seq_len(m)], function(x) if (is.null(x)) NA_real_ else as.numeric(x), numeric(1))
            return(out)
          }
        }
        rep(NA_real_, length(time_utc))
      }
      
      return(tibble(
        timestamp = with_tz(time_utc, TZ_LOCAL),
        TL = pull_anycase('tl'),
        RF = pull_anycase('rf')
      ))
    }
    
    last_msg <- tryCatch(resp_body_string(resp), error = function(e) '')
  }
  
  stop('GeoSphere klima-v2-10min request failed. HTTP ', resp_status(resp), '. Response: ', last_msg)
}

get_station_tlrf <- function(start_date, end_date, station_id, source) {
  if (identical(source, 'GeoSphere')) return(get_geosphere_station_tlrf(start_date, end_date, station_id))
  if (identical(source, 'LWD'))      return(get_lwd_station_tlrf(start_date, end_date, station_id))
  stop('Unknown source: ', source)
}

station_cache_file <- function(start_date, end_date, station_id, source) {
  seas <- season_label(as.Date(end_date))
  f <- file.path(DIR_STATION, sprintf('station_%s_%s_%s.rds', source, station_id, seas))
  f
}

get_station_tlrf_cached <- function(start_date, end_date, station_id, source, verbose = TRUE) {
  f <- station_cache_file(start_date, end_date, station_id, source)
  if (file.exists(f) && file.info(f)$size > 0) {
    if (verbose) message('Station ', station_id, ' (', source, '): cache hit')
    log_msg(2, 'Station cache file -> ', f)
    return(readRDS(f))
  }
  
  if (verbose) message('Station ', station_id, ' (', source, '): cache miss (initial)')
  log_msg(2, 'Station cache file -> ', f)
  
  dat <- get_station_tlrf(start_date, end_date, station_id, source)
  saveRDS(dat, f)
  dat
}

# =====================================================================
# INCA point timeseries (hourly UU/VV/GL) + cache
# =====================================================================
parse_inca_timeseries_csv_file <- function(path) {
  df <- read_geosphere_csv(path)
  if (is.null(df) || nrow(df) == 0) {
    return(tibble(time = as.POSIXct(character(), tz = TZ_LOCAL), UU = numeric(), VV = numeric(), GL = numeric()))
  }
  
  nms <- names(df)
  nms_low <- tolower(nms)
  time_idx <- which(grepl('time|timestamp|date', nms_low))[1]
  if (is.na(time_idx)) stop('INCA CSV: keine Zeitspalte in ', basename(path))
  time_col <- nms[time_idx]
  
  pick <- function(target) {
    i <- which(nms_low == tolower(target)); if (length(i)) return(nms[i[1]])
    i <- which(grepl(paste0('^', tolower(target), '($|[^a-z0-9])'), nms_low)); if (length(i)) return(nms[i[1]])
    i <- which(grepl(tolower(target), nms_low)); if (length(i)) return(nms[i[1]])
    NA_character_
  }
  
  cUU <- pick('UU'); cVV <- pick('VV'); cGL <- pick('GL')
  
  tibble(
    time_utc = parse_time_any_utc(df[[time_col]]),
    UU = if (!is.na(cUU)) suppressWarnings(as.numeric(df[[cUU]])) else NA_real_,
    VV = if (!is.na(cVV)) suppressWarnings(as.numeric(df[[cVV]])) else NA_real_,
    GL = if (!is.na(cGL)) suppressWarnings(as.numeric(df[[cGL]])) else NA_real_
  ) %>%
    filter(!is.na(time_utc)) %>%
    mutate(time = with_tz(time_utc, TZ_LOCAL)) %>%
    select(time, UU, VV, GL)
}

download_inca_point_key_ts <- function(inca_key, lat, lon, start_date, end_date, base_dir = PATH_INCA_DIR, verbose = TRUE) {
  out_dir <- file.path(base_dir, paste0('key_', inca_key))
  safe_mkdir(out_dir)
  
  cs <- as.Date(start_date); ce <- as.Date(end_date)
  outfile <- file.path(out_dir, sprintf('inca_%s_%s_%s.csv', inca_key, format(cs, '%Y%m%d'), format(ce, '%Y%m%d')))
  if (file.exists(outfile) && file.info(outfile)$size > 0) return(outfile)
  
  base_url <- 'https://dataset.api.hub.geosphere.at/v1/timeseries/historical/inca-v1-1h-1km'
  start_time <- sprintf('%sT00:00', format(cs, '%Y-%m-%d'))
  end_time   <- sprintf('%sT23:00', format(ce, '%Y-%m-%d'))
  
  if (verbose) message('INCA ', inca_key, ': download ', cs, '..', ce)
  if (LOG_LEVEL >= 3) log_msg(3, 'INCA request: lat_lon=', paste0(lat, ',', lon), ' | start=', start_time, ' | end=', end_time)
  
  resp <- request(base_url) |>
    req_retry(max_tries = 5) |>
    req_error(is_error = function(resp) FALSE) |>
    req_url_query(
      parameters    = 'UU,VV,GL',
      start         = start_time,
      end           = end_time,
      lat_lon       = paste0(lat, ',', lon),
      output_format = 'csv'
    ) |>
    req_user_agent('icefall-model/1.0 (R httr2)') |>
    req_perform()
  
  if (resp_status(resp) >= 400) {
    msg <- tryCatch(resp_body_string(resp), error = function(e) '')
    stop('INCA download failed (HTTP ', resp_status(resp), '). ', msg)
  }
  
  writeLines(resp_body_string(resp), outfile, useBytes = TRUE)
  Sys.sleep(0.2)
  outfile
}

get_inca_point_hourly_key <- function(inca_key, start_date, end_date, lon, lat, path_dir = PATH_INCA_DIR, verbose = TRUE) {
  f <- download_inca_point_key_ts(inca_key, lat, lon, start_date, end_date, base_dir = path_dir, verbose = verbose)
  
  parse_inca_timeseries_csv_file(f) %>%
    arrange(time) %>%
    group_by(time) %>%
    summarise(UU = mean(UU, na.rm = TRUE), VV = mean(VV, na.rm = TRUE), GL = mean(GL, na.rm = TRUE), .groups = 'drop') %>%
    mutate(
      FF_inca = sqrt(UU^2 + VV^2),
      DD_inca = (atan2(UU, VV) * 180/pi + 180) %% 360,
      GLOW_inca = GL,
      inca_key = inca_key
    ) %>%
    select(inca_key, time, FF_inca, DD_inca, GLOW_inca)
}

# =====================================================================
# NWP forecast point (hourly) + cache
# =====================================================================
grad_to_glow_wm2_vec <- function(grad_ws_m2) {
  g <- as.numeric(grad_ws_m2)
  g[!is.finite(g)] <- NA_real_
  if (all(is.na(g))) return(g)
  g <- zoo::na.locf(g, na.rm = FALSE)
  g <- zoo::na.locf(g, na.rm = FALSE, fromLast = TRUE)
  inc <- c(g[1], diff(g))
  reset <- inc < 0
  inc[reset] <- g[reset]
  inc <- pmax(0, inc)
  inc / 3600
}

get_nwp_metadata_cached <- function(base_dir = PATH_NWP_DIR, max_age_min = 20) {
  safe_mkdir(base_dir)
  cache <- file.path(base_dir, 'nwp_metadata_cache.json')
  
  if (file.exists(cache)) {
    age_min <- as.numeric(difftime(Sys.time(), file.info(cache)$mtime, units = 'mins'))
    if (is.finite(age_min) && age_min <= max_age_min) {
      txt <- paste(readLines(cache, warn = FALSE), collapse = "
")
      return(jsonlite::fromJSON(txt, simplifyVector = TRUE))
    }
  }
  
  meta_url <- 'https://dataset.api.hub.geosphere.at/v1/timeseries/forecast/nwp-v1-1h-2500m/metadata'
  resp <- request(meta_url) |> req_user_agent('icefall-model/1.0 (R httr2)') |> req_perform()
  txt <- resp_body_string(resp)
  writeLines(txt, cache, useBytes = TRUE)
  jsonlite::fromJSON(txt, simplifyVector = TRUE)
}

parse_nwp_timeseries_csv_file <- function(path) {
  df <- read_geosphere_csv(path)
  if (is.null(df) || nrow(df) == 0) return(tibble(time = as.POSIXct(character(), tz = TZ_LOCAL)))
  
  nms <- names(df)
  nms_low <- tolower(nms)
  time_idx <- which(grepl('time|timestamp|date', nms_low))[1]
  if (is.na(time_idx)) stop('NWP CSV: keine Zeitspalte in ', basename(path))
  time_col <- nms[time_idx]
  
  pick <- function(target) {
    i <- which(nms_low == tolower(target)); if (length(i)) return(nms[i[1]])
    i <- which(grepl(paste0('^', tolower(target), '($|[^a-z0-9])'), nms_low)); if (length(i)) return(nms[i[1]])
    i <- which(grepl(tolower(target), nms_low)); if (length(i)) return(nms[i[1]])
    NA_character_
  }
  
  cols <- list(
    t2m  = pick('t2m'),
    rh2m = pick('rh2m'),
    u10m = pick('u10m'),
    v10m = pick('v10m'),
    grad = pick('grad')
  )
  
  out <- tibble(time_utc = parse_time_any_utc(df[[time_col]]))
  for (nm in names(cols)) {
    cc <- cols[[nm]]
    out[[nm]] <- if (!is.na(cc)) suppressWarnings(as.numeric(df[[cc]])) else NA_real_
  }
  
  out %>%
    filter(!is.na(time_utc)) %>%
    mutate(time = with_tz(time_utc, TZ_LOCAL)) %>%
    select(time, t2m, rh2m, u10m, v10m, grad)
}

download_nwp_point_key_fc <- function(nwp_key, lat, lon, start_time_local, end_time_local, forecast_offset = 0, base_dir = PATH_NWP_DIR, verbose = TRUE) {
  safe_mkdir(base_dir)
  out_dir <- file.path(base_dir, paste0('key_', nwp_key))
  safe_mkdir(out_dir)
  
  meta <- get_nwp_metadata_cached(base_dir = base_dir)
  last_ref <- meta[['last_forecast_reftime']]
  ref_tag <- gsub('[^0-9T-]', '', gsub('[:+]', '', last_ref))
  
  start_utc <- with_tz(as.POSIXct(start_time_local, tz = TZ_LOCAL), 'UTC')
  end_utc   <- with_tz(as.POSIXct(end_time_local,   tz = TZ_LOCAL), 'UTC')
  
  outfile <- file.path(
    out_dir,
    sprintf('nwp_%s_ref%s_%s_%s.csv',
            nwp_key, ref_tag,
            format(start_utc, '%Y%m%d%H%M'),
            format(end_utc,   '%Y%m%d%H%M')
    )
  )
  
  if (file.exists(outfile) && file.info(outfile)$size > 0) return(outfile)
  
  base_url <- 'https://dataset.api.hub.geosphere.at/v1/timeseries/forecast/nwp-v1-1h-2500m'
  start_q <- format(start_utc, '%Y-%m-%dT%H:%M')
  end_q   <- format(end_utc,   '%Y-%m-%dT%H:%M')
  
  if (verbose) message('NWP ', nwp_key, ': download ', start_q, ' .. ', end_q)
  if (LOG_LEVEL >= 3) log_msg(3, 'NWP request: lat_lon=', paste0(lat, ',', lon), ' | start=', start_q, ' | end=', end_q, ' | off=', forecast_offset)
  
  resp <- request(base_url) |>
    req_retry(max_tries = 5) |>
    req_error(is_error = function(r) FALSE) |>
    req_url_query(
      parameters = 't2m,rh2m,u10m,v10m,grad',
      start = start_q,
      end   = end_q,
      lat_lon = paste0(lat, ',', lon),
      forecast_offset = as.integer(forecast_offset),
      output_format = 'csv'
    ) |>
    req_user_agent('icefall-model/1.0 (R httr2)') |>
    req_perform()
  
  if (resp_status(resp) >= 400) {
    msg <- tryCatch(resp_body_string(resp), error = function(e) '')
    stop('NWP download failed (HTTP ', resp_status(resp), '). ', msg)
  }
  
  writeLines(resp_body_string(resp), outfile, useBytes = TRUE)
  Sys.sleep(0.2)
  outfile
}

get_nwp_point_forecast_hourly_key <- function(nwp_key, start_time_local, end_time_local, lon, lat, base_dir = PATH_NWP_DIR, forecast_offset = 0, verbose = TRUE) {
  f <- download_nwp_point_key_fc(nwp_key, lat, lon, start_time_local, end_time_local, forecast_offset = forecast_offset, base_dir = base_dir, verbose = verbose)
  
  hr <- parse_nwp_timeseries_csv_file(f) %>%
    arrange(time) %>%
    distinct(time, .keep_all = TRUE)
  
  if (nrow(hr) == 0) {
    return(tibble(nwp_key = nwp_key, time = as.POSIXct(character(), tz = TZ_LOCAL), TL = numeric(), RF = numeric(), FF = numeric(), DD = numeric(), GLOW = numeric()))
  }
  
  glow <- grad_to_glow_wm2_vec(hr$grad)
  
  tibble(
    nwp_key = nwp_key,
    time = hr$time,
    TL   = hr$t2m,
    RF   = hr$rh2m,
    FF   = sqrt(hr$u10m^2 + hr$v10m^2),
    DD   = (atan2(hr$u10m, hr$v10m) * 180/pi + 180) %% 360,
    GLOW = glow
  )
}

# =====================================================================
# Pre-download station caches (once per station block)
# =====================================================================
station_blocks <- assign_run %>%
  filter(is.finite(uid)) %>%
  count(source, station_id, name = 'n_uids', sort = TRUE) %>%
  arrange(source, station_id)

if (nrow(station_blocks) > 0) {
  for (i in seq_len(nrow(station_blocks))) {
    s <- as.character(station_blocks$station_id[i])
    src <- as.character(station_blocks$source[i])
    log_header(paste0('⛅ Station-Block: ', s, ' (', src, ') | uids=', station_blocks$n_uids[i]), level = 1)
    try(get_station_tlrf_cached(START_DATE, END_DATE, s, src, verbose = TRUE), silent = TRUE)
    
    if (LOG_LEVEL >= 2) {
      u_preview <- assign_run %>% filter(.data$station_id == s, .data$source == src) %>% pull(.data$uid) %>% unique()
      u_preview <- head(sort(u_preview), 12)
      log_msg(2, 'UID preview: ', paste(u_preview, collapse = ', '), ifelse(length(u_preview) == 12, ' ...', ''))
    }
  }
}

# =====================================================================
# Build ALL INCA points (hourly) by key -> then expand to uid
# =====================================================================
uid2inca <- assign_run %>%
  distinct(uid, ice_lat, ice_lon) %>%
  mutate(inca_key = coord_key('inca', ice_lat, ice_lon, decimals = INCA_KEY_DECIMALS)) %>%
  filter(is.finite(uid), !is.na(.data$inca_key))

if (LOG_LEVEL >= 2) {
  log_header('INCA DEDUP SUMMARY', level = 2)
  log_msg(2, 'UIDs with coords: ', n_distinct(uid2inca$uid))
  log_msg(2, 'Unique inca_key: ', n_distinct(uid2inca$inca_key))
  log_msg(2, 'Reuse factor (uids/keys): ', round(n_distinct(uid2inca$uid) / max(1, n_distinct(uid2inca$inca_key)), 2))
  print(uid2inca %>% count(inca_key, sort = TRUE) %>% head(10))
}

inca_jobs <- uid2inca %>% distinct(inca_key, ice_lat, ice_lon) %>% arrange(inca_key)
inca_points_rds <- file.path(DIR_CACHE, sprintf('inca_points_bykey_%s_%s_d%d.rds', format(START_DATE, '%Y%m%d'), format(END_DATE, '%Y%m%d'), INCA_KEY_DECIMALS))

inca_by_key <- if (file.exists(inca_points_rds) && file.info(inca_points_rds)$size > 0) {
  message('INCA points(by key): cache hit -> ', inca_points_rds)
  readRDS(inca_points_rds)
} else {
  message('INCA points(by key): cache miss (build) -> ', inca_points_rds)
  
  worker_fun <- function(i) {
    k <- inca_jobs$inca_key[i]
    lat <- inca_jobs$ice_lat[i]
    lon <- inca_jobs$ice_lon[i]
    get_inca_point_hourly_key(k, START_DATE, END_DATE, lon = lon, lat = lat, path_dir = PATH_INCA_DIR, verbose = (LOG_LEVEL >= 3))
  }
  
  idx <- seq_len(nrow(inca_jobs))
  out_list <- if (.Platform$OS.type == 'unix' && N_WORKERS > 1) {
    parallel::mclapply(idx, worker_fun, mc.cores = N_WORKERS)
  } else {
    lapply(idx, worker_fun)
  }
  
  out <- bind_rows(out_list) %>% arrange(inca_key, time)
  saveRDS(out, inca_points_rds)
  out
}

inca_all <- uid2inca %>%
  select(uid, inca_key) %>%
  left_join(inca_by_key, by = 'inca_key') %>%
  select(uid, time, FF_inca, DD_inca, GLOW_inca) %>%
  arrange(uid, time)

# =====================================================================
# Build ALL NWP forecast points (hourly) by key -> then expand to uid
# =====================================================================
uid2nwp <- assign_run %>%
  distinct(uid, ice_lat, ice_lon) %>%
  mutate(nwp_key = coord_key('nwp', ice_lat, ice_lon, decimals = NWP_KEY_DECIMALS)) %>%
  filter(is.finite(uid), !is.na(.data$nwp_key))

if (LOG_LEVEL >= 2) {
  log_header('NWP DEDUP SUMMARY', level = 2)
  log_msg(2, 'UIDs with coords: ', n_distinct(uid2nwp$uid))
  log_msg(2, 'Unique nwp_key: ', n_distinct(uid2nwp$nwp_key))
  log_msg(2, 'Reuse factor (uids/keys): ', round(n_distinct(uid2nwp$uid) / max(1, n_distinct(uid2nwp$nwp_key)), 2))
  print(uid2nwp %>% count(nwp_key, sort = TRUE) %>% head(10))
}

nwp_jobs <- uid2nwp %>% distinct(nwp_key, ice_lat, ice_lon) %>% arrange(nwp_key)

meta <- get_nwp_metadata_cached(base_dir = PATH_NWP_DIR)
ref_tag <- gsub('[^0-9T-]', '', gsub('[:+]', '', meta[['last_forecast_reftime']]))

nwp_points_rds <- file.path(DIR_CACHE, sprintf(
  'nwp_points_bykey_ref%s_%s_%s_d%d.rds',
  ref_tag,
  format(with_tz(HIST_END_LOCAL, 'UTC'), '%Y%m%d%H%M'),
  format(with_tz(FC_END_LOCAL,   'UTC'), '%Y%m%d%H%M'),
  NWP_KEY_DECIMALS
))

nwp_by_key <- if (file.exists(nwp_points_rds) && file.info(nwp_points_rds)$size > 0) {
  message('NWP points(by key): cache hit -> ', nwp_points_rds)
  readRDS(nwp_points_rds)
} else {
  message('NWP points(by key): cache miss (build) -> ', nwp_points_rds)
  
  worker_fun <- function(i) {
    k <- nwp_jobs$nwp_key[i]
    lat <- nwp_jobs$ice_lat[i]
    lon <- nwp_jobs$ice_lon[i]
    get_nwp_point_forecast_hourly_key(k, HIST_END_LOCAL, FC_END_LOCAL, lon = lon, lat = lat, base_dir = PATH_NWP_DIR, verbose = (LOG_LEVEL >= 3))
  }
  
  idx <- seq_len(nrow(nwp_jobs))
  out_list <- if (.Platform$OS.type == 'unix' && N_WORKERS > 1) {
    parallel::mclapply(idx, worker_fun, mc.cores = N_WORKERS)
  } else {
    lapply(idx, worker_fun)
  }
  
  out <- bind_rows(out_list) %>% arrange(nwp_key, time)
  saveRDS(out, nwp_points_rds)
  out
}

nwp_all <- uid2nwp %>%
  select(uid, nwp_key) %>%
  left_join(nwp_by_key, by = 'nwp_key') %>%
  select(uid, time, TL, RF, FF, DD, GLOW) %>%
  arrange(uid, time)

# =====================================================================
# Build ONE UID (model + plot)
# =====================================================================
mean_na <- function(x) { m <- mean(x, na.rm = TRUE); if (is.nan(m)) NA_real_ else m }

build_one_uid <- function(uid, inca_all, nwp_all, verbose = TRUE) {
  row_uid <- assign_run %>% filter(.data$uid == uid) %>% slice(1)
  if (nrow(row_uid) == 0) stop('uid nicht gefunden: ', uid)
  
  station_id <- as.character(row_uid$station_id)
  source     <- as.character(row_uid$source)
  dist_km    <- to_num(row_uid$dist_km)
  dz_m       <- to_num(row_uid$elev_diff_m)  # ice - station
  ice_lon    <- to_num(row_uid$ice_lon)
  ice_lat    <- to_num(row_uid$ice_lat)
  
  ice_name <- NA_character_
  if ('icefall_name' %in% names(row_uid)) ice_name <- row_uid$icefall_name
  if (is.null(ice_name) || is.na(ice_name) || !nzchar(ice_name)) {
    if ('ice_name' %in% names(row_uid)) ice_name <- row_uid$ice_name
  }
  if (is.null(ice_name) || is.na(ice_name) || !nzchar(ice_name)) ice_name <- row_uid$name
  if (is.null(ice_name) || is.na(ice_name) || !nzchar(ice_name)) ice_name <- 'Eisfall'
  
  ice_alt_m <- if ('icefall_elev_m' %in% names(row_uid)) to_num(row_uid$icefall_elev_m) else NA_real_
  ice_fallheight_m <- if ('icefall_height_m' %in% names(row_uid)) to_num(row_uid$icefall_height_m) else NA_real_
  
  # --- Sun + Wind LUT (fallbacks if missing) ---
  sun_uid <- sun_all_run %>%
    filter(.data$uid == uid) %>%
    mutate(
      date = as.Date(date),
      sunrise_topo = parse_time_any(sunrise_topo, tz = TZ_LOCAL),
      sunset_topo  = parse_time_any(sunset_topo,  tz = TZ_LOCAL),
      sun_hours_topo = to_num(sun_hours_topo)
    ) %>%
    select(date, sunrise_topo, sunset_topo, sun_hours_topo) %>%
    distinct(date, .keep_all = TRUE) %>%
    filter(date >= START_DATE, date <= END_DATE_EXT)
  
  wind_uid <- wind_lut_run %>%
    filter(.data$uid == uid) %>%
    transmute(
      dir_deg = as.numeric(.data$dir_deg),
      wind_vuln_0_9 = suppressWarnings(as.integer(.data$wind_vuln_0_9))
    ) %>%
    filter(is.finite(.data$dir_deg)) %>%
    distinct(.data$dir_deg, .keep_all = TRUE)
  
  # --- Station TL/RF (10-min) cached ---
  wx10 <- get_station_tlrf_cached(START_DATE, END_DATE, station_id, source, verbose = verbose) %>%
    mutate(
      timestamp = with_tz(as.POSIXct(timestamp, tz = TZ_LOCAL), TZ_LOCAL),
      TL = to_num(TL),
      RF = to_num(RF)
    )
  
  if (all(is.na(wx10$TL))) stop('TL fehlt komplett (', station_id, ').')
  if (all(is.na(wx10$RF))) stop('RF fehlt komplett (', station_id, ').')
  
  step_str <- paste0(MODEL_STEP_MIN, ' mins')
  
  wx <- wx10 %>%
    mutate(time = floor_date(timestamp, unit = step_str)) %>%
    group_by(time) %>%
    summarise(TL = mean(TL, na.rm = TRUE), RF = mean(RF, na.rm = TRUE), .groups = 'drop') %>%
    tidyr::complete(time = seq(min(time), max(time), by = step_str)) %>%
    arrange(time) %>%
    mutate(TL = fill1(TL), RF = fill1(RF))
  
  # --- INCA override: FF/DD/GLOW (hourly -> 10-min LOCF) ---
  inca_hr <- inca_all %>% filter(.data$uid == uid) %>% arrange(time)
  
  wx <- wx %>%
    left_join(inca_hr, by = 'time') %>%
    arrange(time) %>%
    mutate(
      FF_inca   = zoo::na.locf(FF_inca,   na.rm = FALSE),
      DD_inca   = zoo::na.locf(DD_inca,   na.rm = FALSE),
      GLOW_inca = zoo::na.locf(GLOW_inca, na.rm = FALSE),
      FF_inca   = zoo::na.locf(FF_inca,   na.rm = FALSE, fromLast = TRUE),
      DD_inca   = zoo::na.locf(DD_inca,   na.rm = FALSE, fromLast = TRUE),
      GLOW_inca = zoo::na.locf(GLOW_inca, na.rm = FALSE, fromLast = TRUE),
      FF   = coalesce(FF_inca, 0),
      DD   = coalesce(DD_inca, 0),
      GLOW = coalesce(GLOW_inca, 0)
    ) %>%
    select(-FF_inca, -DD_inca, -GLOW_inca)
  
  # only keep history up to now
  wx <- wx %>% filter(time <= HIST_END_LOCAL)
  
  HIST_END <- max(wx$time, na.rm = TRUE)
  FC_END   <- HIST_END + hours(FORECAST_HOURS)
  
  # --- NWP forecast (hourly -> 10-min LOCF) ---
  nwp_hr <- nwp_all %>% filter(.data$uid == uid) %>% arrange(time)
  
  wx_fc <- tibble(time = seq(HIST_END + minutes(MODEL_STEP_MIN), FC_END, by = step_str)) %>%
    mutate(time_hr = floor_date(time, 'hour')) %>%
    left_join(nwp_hr %>% rename(time_hr = time), by = 'time_hr') %>%
    select(-time_hr) %>%
    arrange(time) %>%
    mutate(
      TL   = zoo::na.locf(TL,   na.rm = FALSE),
      RF   = zoo::na.locf(RF,   na.rm = FALSE),
      FF   = zoo::na.locf(FF,   na.rm = FALSE),
      DD   = zoo::na.locf(DD,   na.rm = FALSE),
      GLOW = zoo::na.locf(GLOW, na.rm = FALSE),
      TL   = zoo::na.locf(TL,   na.rm = FALSE, fromLast = TRUE),
      RF   = zoo::na.locf(RF,   na.rm = FALSE, fromLast = TRUE),
      FF   = zoo::na.locf(FF,   na.rm = FALSE, fromLast = TRUE),
      DD   = zoo::na.locf(DD,   na.rm = FALSE, fromLast = TRUE),
      GLOW = zoo::na.locf(GLOW, na.rm = FALSE, fromLast = TRUE)
    )
  
  wx <- bind_rows(wx %>% mutate(is_forecast = FALSE), wx_fc %>% mutate(is_forecast = TRUE)) %>% arrange(time)
  
  # --- Join topo sun + wind vulnerability ---
  wx <- wx %>%
    mutate(date = as.Date(time))
  
  if (nrow(sun_uid) > 0) {
    wx <- wx %>% left_join(sun_uid, by = 'date')
  } else {
    wx <- wx %>% mutate(sunrise_topo = as.POSIXct(NA, tz = TZ_LOCAL), sunset_topo = as.POSIXct(NA, tz = TZ_LOCAL), sun_hours_topo = NA_real_)
  }
  
  wx <- wx %>%
    mutate(
      topo_sun_fac = ifelse(!is.na(sunrise_topo) & !is.na(sunset_topo) & time >= sunrise_topo & time < sunset_topo, 1, 0),
      dd_bin = bin5(DD)
    )
  
  if (nrow(wind_uid) > 0) {
    wx <- wx %>% left_join(wind_uid, by = c('dd_bin' = 'dir_deg'))
  } else {
    wx <- wx %>% mutate(wind_vuln_0_9 = 9L)
  }
  
  # ensure wind_vuln_0_9 exists + stable type
  if (!'wind_vuln_0_9' %in% names(wx)) wx$wind_vuln_0_9 <- NA_integer_
  
  wx <- wx %>%
    mutate(
      wind_vuln_0_9 = suppressWarnings(as.integer(wind_vuln_0_9)),
      wind_vuln_0_9 = dplyr::coalesce(wind_vuln_0_9, 9L),
      # clamp to expected domain
      wind_vuln_0_9 = pmin(9L, pmax(0L, wind_vuln_0_9)),
      wind_vuln = pmin(1, pmax(0, wind_vuln_0_9 / 9))
    )
  
  # --- Ice model ---
  ice_params <- list(albedo = 0.50, Hmax_m = 0.90, H0_m = 0.20)
  coef <- list(
    lapse_K_per_m      = 0.0065,
    growth_mm_per_C_h  = 0.50,
    melt_mm_per_C_h    = 0.70,
    rad_melt_mm_per_MJ = 0.35,
    k_wind             = 0.06,
    k_dry              = 0.25,
    wind_cap_ms        = 15
  )
  
  wx <- wx %>%
    mutate(
      dz_eff = ifelse(is_forecast, 0, dz_m),
      TLz    = TL - coef$lapse_K_per_m * dz_eff,
      FDH = pmax(0, -TLz),
      PDH = pmax(0,  TLz),
      GLOW = ifelse(is.finite(GLOW), GLOW, 0),
      SW_MJ_step = GLOW * W2MJ_STEP * topo_sun_fac * (1 - ice_params$albedo),
      FF_eff   = pmin(coef$wind_cap_ms, pmax(0, FF)),
      wind_fac = 1 + coef$k_wind * FF_eff * wind_vuln,
      dry_fac  = 1 + coef$k_dry  * pmax(0, 1 - RF/100),
      base_growth_mm_step = coef$growth_mm_per_C_h * FDH * DT_H * wind_fac * dry_fac,
      base_melt_mm_step   = coef$melt_mm_per_C_h * PDH * DT_H * wind_fac + coef$rad_melt_mm_per_MJ * SW_MJ_step
    )
  
  thickness_mm <- numeric(nrow(wx))
  thickness_mm[1] <- 50
  for (i in 2:nrow(wx)) {
    Hprev_m <- thickness_mm[i-1] / 1000
    iso <- exp(-Hprev_m / ice_params$H0_m)
    cap <- pmax(0, 1 - Hprev_m / ice_params$Hmax_m)
    growth <- wx$base_growth_mm_step[i] * iso * cap
    melt   <- wx$base_melt_mm_step[i]
    thickness_mm[i] <- max(0, thickness_mm[i-1] + (growth - melt))
  }
  
  mod <- wx %>%
    mutate(
      thickness_m = thickness_mm / 1000,
      station_id = station_id,
      source = source,
      dist_km = dist_km,
      dz_m = dz_m
    )
  
  # --- Climbability ---
  H_MIN <- 0.10; H_OPT <- 0.50
  T_OPT <- -4;  T_MIN <- -20; T_MAX <- 0
  RANGE_T <- max(T_OPT - T_MIN, T_MAX - T_OPT)
  T3_OPT <- -6; T3_MIN <- -20; T3_MAX <- -1
  RANGE_T3 <- max(T3_OPT - T3_MIN, T3_MAX - T3_OPT)
  RH_OPT <- 0.70; RH_SIG <- 0.20
  WIN_72H <- as.integer(72 * 60 / MODEL_STEP_MIN)
  
  score_T_fun_vec <- function(Tv, Topt, Tmin, Tmax, rangeT) {
    s <- 1 - abs(Tv - Topt) / rangeT
    s[Tv <= Tmin | Tv >= Tmax] <- 0
    s[!is.finite(s)] <- NA_real_
    pmax(0, s)
  }
  
  mod <- mod %>%
    mutate(
      TLz_72h = zoo::rollapplyr(TLz, width = WIN_72H, FUN = function(x) mean(x, na.rm = TRUE), fill = NA_real_, partial = TRUE),
      score_h  = pmin(1, pmax(0, (thickness_m - H_MIN) / (H_OPT - H_MIN))),
      score_T  = score_T_fun_vec(TLz,     T_OPT,  T_MIN,  T_MAX,  RANGE_T),
      score_T3 = score_T_fun_vec(TLz_72h,  T3_OPT, T3_MIN, T3_MAX, RANGE_T3),
      score_RH = exp(-((RF/100) - RH_OPT)^2 / (2 * RH_SIG^2)),
      climbability = score_h * score_T * score_T3 * score_RH,
      climbability = ifelse(thickness_m < H_MIN, NA_real_, climbability),
      climbability = pmin(1, pmax(0, climbability)),
      date = as.Date(time)
    )
  
  climb_hist_daily <- mod %>%
    filter(!is_forecast) %>%
    group_by(date) %>%
    summarise(
      time = as.POSIXct(paste0(first(date), ' 12:00:00'), tz = TZ_LOCAL),
      climbability = mean_na(climbability),
      .groups = 'drop'
    )
  
  # --- Save CSV ---
  out_csv <- file.path(DIR_MODEL, sprintf('model_uid%03d.csv', uid))
  write_csv(mod, out_csv)
  
  # --- Plot (split hist + forecast) ---
  has_fc <- any(mod$is_forecast %in% TRUE)
  x_min <- as.POSIXct(START_DATE, tz = TZ_LOCAL)
  x_max <- max(mod$time, na.rm = TRUE)
  
  y_min <- min(mod$thickness_m, na.rm = TRUE)
  y_max <- max(mod$thickness_m, na.rm = TRUE)
  Y_DEN <- max(1e-6, y_max - y_min)
  
  mod <- mod %>% mutate(climb_y = y_min + climbability * Y_DEN)
  climb_hist_daily <- climb_hist_daily %>% mutate(climb_y = y_min + climbability * Y_DEN)
  
  if (!has_fc) {
    plt <- ggplot(mod, aes(time, thickness_m)) +
      geom_line(linewidth = 0.9) +
      geom_line(data = climb_hist_daily, aes(time, climb_y), inherit.aes = FALSE, linewidth = 0.8, color = 'red', na.rm = TRUE) +
      coord_cartesian(xlim = c(x_min, x_max), ylim = c(y_min, y_max)) +
      scale_x_datetime(date_breaks = '1 month', date_labels = '%b', timezone = TZ_LOCAL, guide = guide_axis(check.overlap = TRUE)) +
      scale_y_continuous(name = 'Eisdicke (m)', sec.axis = sec_axis(~(. - y_min) / Y_DEN, name = 'Climbability (0–1)')) +
      labs(
        title = paste0('Modellierte Eisdicke – ', ice_name),
        subtitle = paste(
          c(
            if (!is.na(ice_fallheight_m)) paste0('Eisfallhöhe: ', round(ice_fallheight_m, 0), ' m'),
            if (!is.na(ice_alt_m)) paste0('Höhe: ', round(ice_alt_m, 0), ' m'),
            paste0('Station: ', station_id, ' (', source, ')'),
            paste0('dist ', round(dist_km, 2), ' km'),
            paste0('dz ', round(dz_m, 0), ' m')
          ),
          collapse = ' | '
        ),
        x = 'Zeit'
      ) +
      theme_minimal(base_size = 12) +
      theme(axis.text.x = element_text(size = 9, lineheight = 0.95))
  } else {
    forecast_start <- min(mod$time[mod$is_forecast], na.rm = TRUE)
    mod_hist <- mod %>% filter(time >= x_min, time < forecast_start)
    mod_fc   <- mod %>% filter(time >= forecast_start)
    
    # sun bands in forecast panel
    sun_rects_fc <- tibble(xmin = as.POSIXct(character(), tz = TZ_LOCAL), xmax = as.POSIXct(character(), tz = TZ_LOCAL), ymin = numeric(), ymax = numeric())
    if (nrow(mod_fc) > 0) {
      tmp <- mod_fc %>% arrange(time) %>% filter(!is.na(time)) %>% mutate(is_sun = topo_sun_fac == 1, run = cumsum(is_sun != dplyr::lag(is_sun, default = dplyr::first(is_sun))))
      tmp_sun <- tmp %>% filter(is_sun)
      if (nrow(tmp_sun) > 0) {
        sun_rects_fc <- tmp_sun %>% group_by(run) %>% reframe(xmin = min(time), xmax = max(time) + minutes(MODEL_STEP_MIN), ymin = -Inf, ymax = Inf)
      }
    }
    
    peak_fc <- mod_fc %>%
      filter(is.finite(climbability)) %>%
      group_by(date) %>%
      slice_max(order_by = climbability, n = 1, with_ties = FALSE) %>%
      ungroup() %>%
      transmute(peak_time = time, peak_label = format(time, "%d.%m
%H:%M"))
    
    bg <- tibble(xmin = forecast_start, xmax = x_max, ymin = -Inf, ymax = Inf)
    
    p_hist <- ggplot(mod_hist, aes(time, thickness_m)) +
      geom_line(linewidth = 0.9) +
      geom_line(data = climb_hist_daily, aes(time, climb_y), inherit.aes = FALSE, linewidth = 0.85, color = 'red', na.rm = TRUE) +
      coord_cartesian(xlim = c(x_min, forecast_start), ylim = c(y_min, y_max)) +
      scale_x_datetime(date_breaks = '1 month', date_labels = '%b', timezone = TZ_LOCAL, guide = guide_axis(check.overlap = TRUE)) +
      scale_y_continuous(name = 'Eisdicke (m)') +
      theme_minimal(base_size = 12) +
      theme(
        plot.margin = margin(5.5, 2, 5.5, 5.5),
        axis.text.x = element_text(size = 9, lineheight = 0.95),
        axis.title.x = element_blank(),
        axis.text.y.right  = element_blank(),
        axis.ticks.y.right = element_blank(),
        axis.title.y.right = element_blank()
      )
    
    p_fc <- ggplot(mod_fc, aes(time, thickness_m)) +
      geom_rect(data = bg, aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax), inherit.aes = FALSE, fill = 'grey85', alpha = 0.6) +
      geom_rect(data = sun_rects_fc, aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax), inherit.aes = FALSE, fill = 'yellow', alpha = 0.25) +
      geom_line(linewidth = 0.9) +
      geom_line(aes(y = climb_y), linewidth = 0.8, color = 'red', na.rm = TRUE) +
      coord_cartesian(xlim = c(forecast_start, x_max), ylim = c(y_min, y_max)) +
      scale_x_datetime(breaks = peak_fc$peak_time, labels = peak_fc$peak_label, minor_breaks = NULL, timezone = TZ_LOCAL, guide = guide_axis(check.overlap = TRUE)) +
      scale_y_continuous(name = NULL, sec.axis = sec_axis(~(. - y_min) / Y_DEN, name = 'Climbability (0–1)')) +
      theme_minimal(base_size = 12) +
      theme(
        panel.grid.major.x = element_line(),
        panel.grid.minor.x = element_blank(),
        plot.margin = margin(5.5, 18, 5.5, 2),
        axis.title.x = element_blank(),
        axis.text.y  = element_blank(),
        axis.ticks.y = element_blank(),
        axis.title.y = element_blank(),
        axis.text.y.right  = element_text(size = 9),
        axis.ticks.y.right = element_line(),
        axis.title.y.right = element_text(margin = margin(l = 6)),
        axis.text.x = element_text(size = 9, lineheight = 0.95)
      )
    
    plt <- (p_hist + p_fc) +
      patchwork::plot_layout(widths = c(2, 1)) +
      patchwork::plot_annotation(
        title = paste0('Modellierte Eisdicke – ', ice_name),
        subtitle = paste(
          c(
            if (!is.na(ice_alt_m)) paste0('Höhe: ', round(ice_alt_m, 0), ' m'),
            paste0('Station: ', station_id, ' (', source, ')'),
            paste0('dist ', round(dist_km, 2), ' km'),
            paste0('dz ', round(dz_m, 0), ' m'),
            'Forecast (grau)'
          ),
          collapse = ' | '
        ),
        caption = paste0('10-min Modell (dt=', MODEL_STEP_MIN, ' min): FDH/PDH + SW(toposun) + Wind(vuln) + Dryness + Sättigung')
      )
  }
  
  plot_file <- file.path(DIR_PLOTS, sprintf('uid_%03d.png', uid))
  ggsave(filename = plot_file, plot = plt, width = 14, height = 5, units = 'in', dpi = 200, bg = 'white')
  
  if (verbose) {
    message(sprintf('✅ UID %d: CSV -> %s | Plot -> %s', uid, out_csv, plot_file))
  }
  
  invisible(list(csv = out_csv, plot = plot_file))
}

# =====================================================================
# Loop UIDs
# =====================================================================
failed <- integer(0)

for (u in uids) {
  log_header(paste0('📈 Build Plot UID: ', u), level = 1)
  
  if (LOG_LEVEL >= 2) {
    r0 <- assign_run %>% filter(.data$uid == u) %>% slice(1)
    inca_k <- uid2inca$inca_key[match(u, uid2inca$uid)][1]
    nwp_k  <- uid2nwp$nwp_key[match(u, uid2nwp$uid)][1]
    log_msg(2, 'name=', as.character(r0$name),
            ' | station=', as.character(r0$station_id), ' (', as.character(r0$source), ')',
            ' | dist_km=', as.character(r0$dist_km),
            ' | dz_m=', as.character(r0$elev_diff_m),
            ' | inca_key=', inca_k,
            ' | nwp_key=', nwp_k)
  }
  
  ok <- TRUE
  tryCatch({
    build_one_uid(u, inca_all = inca_all, nwp_all = nwp_all, verbose = TRUE)
  }, error = function(e) {
    ok <<- FALSE
    message('❌ UID ', u, ' fehlgeschlagen: ', e$message)
  })
  
  if (!ok) failed <- c(failed, u)
}

if (length(failed) > 0) stop('Plots/ModelRuns fehlgeschlagen für UIDs: ', paste(failed, collapse = ', '))
message("
✅ Alle Plots gebaut: ", length(uids))
