#!/usr/bin/env Rscript

# ============================================================
# validate_temperature_all_tirol_stations.R
# ------------------------------------------------------------
# Für alle Tirol-AWS-Stationen mit Temperaturdaten (tl=TRUE):
# 1) Nimmt je Zielstation die geographisch nächste Nachbarstation.
# 2) Simuliert Zieltemperatur via Standard-Lapse-Rate.
# 3) Bewertet Simulationsgüte (MAE/RMSE/Bias/Korrelation + Rating).
# 4) Analysiert Zusammenhang zwischen Güte und Distanz/Höhendifferenz.
#
# Output:
# - scripts/AWS Validation/validation_all_tirol_stations.csv
# - scripts/AWS Validation/validation_all_tirol_relationships.csv
# - scripts/AWS Validation/validation_all_tirol_summary.txt
# - scripts/AWS Validation/validation_all_tirol_scatter.png
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(lubridate)
  library(httr2)
  library(jsonlite)
})

TZ_LOCAL <- "Europe/Vienna"
LAPSE_K_PER_M <- 0.0065
MIN_OVERLAP <- 30
PATH_STATIONS <- "data/AWS/stations_all.csv"
OUT_DIR <- "scripts/AWS Validation"
OUT_CSV <- file.path(OUT_DIR, "validation_all_tirol_stations.csv")
OUT_REL_CSV <- file.path(OUT_DIR, "validation_all_tirol_relationships.csv")
OUT_SUMMARY <- file.path(OUT_DIR, "validation_all_tirol_summary.txt")
OUT_PNG <- file.path(OUT_DIR, "validation_all_tirol_scatter.png")

args <- commandArgs(trailingOnly = TRUE)
START_DATE <- if (length(args) >= 1) as.Date(args[[1]]) else Sys.Date() - 30
END_DATE <- if (length(args) >= 2) as.Date(args[[2]]) else Sys.Date() - 1
if (length(args) >= 3) {
  MIN_OVERLAP <- suppressWarnings(as.integer(args[[3]]))
}

if (is.na(START_DATE) || is.na(END_DATE) || START_DATE > END_DATE) {
  stop("Ungültiger Zeitraum. Nutzung: Rscript \"scripts/AWS Validation/validate_temperature_all_tirol_stations.R\" YYYY-MM-DD YYYY-MM-DD [MIN_OVERLAP]")
}
if (is.na(MIN_OVERLAP) || MIN_OVERLAP < 10) {
  stop("MIN_OVERLAP muss >= 10 sein.")
}

if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

to_num <- function(x) suppressWarnings(as.numeric(x))

parse_dt_any <- function(x, tz = TZ_LOCAL) {
  x <- as.character(x)
  x[x %in% c("", "NA", "NaN", "NULL")] <- NA_character_
  out <- suppressWarnings(lubridate::dmy_hms(x, tz = tz))
  if (all(is.na(out))) out <- suppressWarnings(lubridate::dmy_hm(x, tz = tz))
  if (all(is.na(out))) out <- suppressWarnings(lubridate::ymd_hms(x, tz = tz))
  if (all(is.na(out))) out <- suppressWarnings(lubridate::ymd_hm(x, tz = tz))
  out
}

season_label <- function(date) {
  y <- year(date)
  m <- month(date)
  ifelse(m >= 10, sprintf("%d_%d", y, y + 1), sprintf("%d_%d", y - 1, y))
}

read_lwd_param <- function(station_code, param, season) {
  url <- sprintf("https://wiski.tirol.gv.at/lawine/produkte/ogd/%s/%s_%s_%s.csv",
                 station_code, station_code, param, season)

  resp <- tryCatch(
    request(url) |>
      req_user_agent("icefall-model/1.0 (R httr2)") |>
      req_perform(),
    error = function(e) NULL
  )
  if (is.null(resp) || resp_status(resp) != 200) return(NULL)

  raw <- tryCatch(resp_body_raw(resp), error = function(e) NULL)
  if (is.null(raw) || length(raw) == 0) return(NULL)

  txt0 <- tryCatch(rawToChar(raw), error = function(e) NA_character_)
  if (is.na(txt0)) return(NULL)
  txt <- iconv(txt0, from = "", to = "UTF-8", sub = "byte")

  tmp <- tryCatch(
    readr::read_delim(file = I(txt), delim = ";", show_col_types = FALSE, progress = FALSE),
    error = function(e) NULL
  )
  if (is.null(tmp) || nrow(tmp) == 0) return(NULL)

  nms <- names(tmp)
  nms_low <- tolower(iconv(nms, from = "", to = "ASCII//TRANSLIT", sub = ""))
  dt_i <- which(grepl("datetime|date_time|zeitstempel|timestamp|datumzeit|datum_zeit", nms_low))
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

get_lwd_station_tl <- function(start_date, end_date, station_code) {
  seasons <- unique(season_label(seq(as.Date(start_date), as.Date(end_date), by = "day")))
  tl <- bind_rows(lapply(seasons, function(seas) read_lwd_param(station_code, "LT", seas)))

  if (nrow(tl) == 0) {
    return(tibble(timestamp = as.POSIXct(character(), tz = TZ_LOCAL), TL = numeric()))
  }

  tl %>%
    transmute(timestamp = as.POSIXct(timestamp, tz = TZ_LOCAL), TL = to_num(value)) %>%
    arrange(timestamp) %>%
    filter(timestamp >= as.POSIXct(start_date, tz = TZ_LOCAL),
           timestamp < as.POSIXct(end_date + 1, tz = TZ_LOCAL))
}

get_geosphere_station_tl <- function(start_date, end_date, station_id) {
  base_url <- "https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-10min"
  start_q <- sprintf("%sT00:00", as.character(as.Date(start_date)))
  end_q <- sprintf("%sT23:50", as.character(as.Date(end_date)))

  for (p in c("tl", "TL")) {
    resp <- tryCatch(
      request(base_url) |>
        req_url_query(
          station_ids = as.character(station_id),
          parameters = p,
          start = start_q,
          end = end_q
        ) |>
        req_user_agent("icefall-model/1.0 (R httr2)") |>
        req_retry(max_tries = 3) |>
        req_error(is_error = function(r) FALSE) |>
        req_perform(),
      error = function(e) NULL
    )

    if (is.null(resp) || resp_status(resp) >= 400) next

    dat <- tryCatch(jsonlite::fromJSON(resp_body_string(resp), simplifyVector = FALSE), error = function(e) NULL)
    if (is.null(dat)) next

    ts_raw <- dat[["timestamps"]]
    time_utc <- suppressWarnings(lubridate::ymd_hms(ts_raw, tz = "UTC"))
    if (all(is.na(time_utc))) {
      ts_fix <- ts_raw
      has_colon_tz <- grepl("([+-][0-9]{2}):([0-9]{2})$", ts_fix)
      ts_fix[has_colon_tz] <- paste0(
        substr(ts_fix[has_colon_tz], 1, nchar(ts_fix[has_colon_tz]) - 3),
        substr(ts_fix[has_colon_tz], nchar(ts_fix[has_colon_tz]) - 1, nchar(ts_fix[has_colon_tz]))
      )
      time_utc <- as.POSIXct(strptime(ts_fix, "%Y-%m-%dT%H:%M%z", tz = "UTC"))
    }

    feat <- dat[["features"]][[1]]
    params <- feat[["properties"]][["parameters"]]

    pull_anycase <- function(name) {
      for (nm in c(name, tolower(name), toupper(name))) {
        p0 <- params[[nm]]
        if (!is.null(p0) && !is.null(p0[["data"]])) {
          lst <- p0[["data"]]
          out <- rep(NA_real_, length(time_utc))
          m <- min(length(lst), length(time_utc))
          out[seq_len(m)] <- vapply(lst[seq_len(m)], function(x) if (is.null(x)) NA_real_ else as.numeric(x), numeric(1))
          return(out)
        }
      }
      rep(NA_real_, length(time_utc))
    }

    return(tibble(timestamp = with_tz(time_utc, TZ_LOCAL), TL = pull_anycase("tl")))
  }

  tibble(timestamp = as.POSIXct(character(), tz = TZ_LOCAL), TL = numeric())
}

get_station_tl <- function(start_date, end_date, station_id, source) {
  if (identical(source, "GeoSphere")) return(get_geosphere_station_tl(start_date, end_date, station_id))
  if (identical(source, "LWD")) return(get_lwd_station_tl(start_date, end_date, station_id))
  tibble(timestamp = as.POSIXct(character(), tz = TZ_LOCAL), TL = numeric())
}

deg2rad <- function(x) x * pi / 180
haversine_km <- function(lon1, lat1, lon2, lat2) {
  r <- 6371
  dlon <- deg2rad(lon2 - lon1)
  dlat <- deg2rad(lat2 - lat1)
  a <- sin(dlat / 2)^2 + cos(deg2rad(lat1)) * cos(deg2rad(lat2)) * sin(dlon / 2)^2
  2 * r * atan2(sqrt(a), sqrt(1 - a))
}

rate_quality <- function(rmse, mae, corv) {
  if (!is.finite(rmse) || !is.finite(mae) || !is.finite(corv)) return("insufficient")
  if (rmse <= 1.0 && mae <= 0.8 && corv >= 0.95) return("sehr_gut")
  if (rmse <= 1.8 && mae <= 1.4 && corv >= 0.85) return("gut")
  if (rmse <= 2.8 && mae <= 2.2 && corv >= 0.70) return("mittel")
  "schwach"
}

stations <- readr::read_csv(PATH_STATIONS, show_col_types = FALSE) %>%
  mutate(
    station_id = as.character(station_id),
    source = as.character(source),
    lon = to_num(lon),
    lat = to_num(lat),
    altitude_m = to_num(altitude_m),
    tl = as.logical(tl)
  ) %>%
  filter(tl, !is.na(station_id), !is.na(source), !is.na(lon), !is.na(lat), !is.na(altitude_m))

if (nrow(stations) < 2) stop("Zu wenige gültige Temperaturstationen gefunden.")

cache <- new.env(parent = emptyenv())
get_tl_cached <- function(station_id, source) {
  key <- paste(station_id, source, sep = "__")
  if (exists(key, envir = cache, inherits = FALSE)) return(get(key, envir = cache, inherits = FALSE))
  dat <- get_station_tl(START_DATE, END_DATE, station_id, source) %>% filter(!is.na(TL))
  assign(key, dat, envir = cache)
  dat
}

message("Starte Validierung für ", nrow(stations), " Tirol-Stationen...")
results <- vector("list", nrow(stations))

for (i in seq_len(nrow(stations))) {
  target <- stations[i, ]
  sid_t <- target$station_id[[1]]

  nearest <- stations %>%
    filter(station_id != sid_t) %>%
    mutate(
      dist_km = haversine_km(lon, lat, target$lon[[1]], target$lat[[1]]),
      delta_alt_m = altitude_m - target$altitude_m[[1]],
      abs_delta_alt_m = abs(delta_alt_m)
    ) %>%
    arrange(dist_km) %>%
    slice(1)

  sid_n <- nearest$station_id[[1]]
  src_n <- nearest$source[[1]]

  message(sprintf("[%d/%d] Ziel=%s | Nächste=%s | Distanz=%.2f km | ΔH=%.0f m",
                  i, nrow(stations), sid_t, sid_n, nearest$dist_km[[1]], nearest$delta_alt_m[[1]]))

  ref <- get_tl_cached(sid_t, target$source[[1]]) %>% rename(TL_ref = TL)
  cmp_station <- get_tl_cached(sid_n, src_n)

  if (nrow(ref) == 0 || nrow(cmp_station) == 0) {
    results[[i]] <- tibble(
      target_station_id = sid_t,
      target_source = target$source[[1]],
      target_altitude_m = target$altitude_m[[1]],
      nearest_station_id = sid_n,
      nearest_source = src_n,
      nearest_altitude_m = nearest$altitude_m[[1]],
      distance_to_nearest_km = nearest$dist_km[[1]],
      delta_altitude_m = nearest$delta_alt_m[[1]],
      abs_delta_altitude_m = nearest$abs_delta_alt_m[[1]],
      n_overlap = 0,
      mae_C = NA_real_,
      rmse_C = NA_real_,
      bias_C = NA_real_,
      cor = NA_real_,
      quality_rating = "insufficient"
    )
    next
  }

  cmp <- cmp_station %>%
    transmute(timestamp, TL_sim = TL - LAPSE_K_PER_M * (target$altitude_m[[1]] - nearest$altitude_m[[1]])) %>%
    inner_join(ref, by = "timestamp")

  n_overlap <- nrow(cmp)
  if (n_overlap < MIN_OVERLAP) {
    results[[i]] <- tibble(
      target_station_id = sid_t,
      target_source = target$source[[1]],
      target_altitude_m = target$altitude_m[[1]],
      nearest_station_id = sid_n,
      nearest_source = src_n,
      nearest_altitude_m = nearest$altitude_m[[1]],
      distance_to_nearest_km = nearest$dist_km[[1]],
      delta_altitude_m = nearest$delta_alt_m[[1]],
      abs_delta_altitude_m = nearest$abs_delta_alt_m[[1]],
      n_overlap = n_overlap,
      mae_C = NA_real_,
      rmse_C = NA_real_,
      bias_C = NA_real_,
      cor = NA_real_,
      quality_rating = "insufficient"
    )
    next
  }

  err <- cmp$TL_sim - cmp$TL_ref
  mae <- mean(abs(err), na.rm = TRUE)
  rmse <- sqrt(mean(err^2, na.rm = TRUE))
  bias <- mean(err, na.rm = TRUE)
  corv <- suppressWarnings(cor(cmp$TL_sim, cmp$TL_ref, use = "complete.obs"))

  results[[i]] <- tibble(
    target_station_id = sid_t,
    target_source = target$source[[1]],
    target_altitude_m = target$altitude_m[[1]],
    nearest_station_id = sid_n,
    nearest_source = src_n,
    nearest_altitude_m = nearest$altitude_m[[1]],
    distance_to_nearest_km = nearest$dist_km[[1]],
    delta_altitude_m = nearest$delta_alt_m[[1]],
    abs_delta_altitude_m = nearest$abs_delta_alt_m[[1]],
    n_overlap = n_overlap,
    mae_C = mae,
    rmse_C = rmse,
    bias_C = bias,
    cor = corv,
    quality_rating = rate_quality(rmse, mae, corv)
  )
}

res <- bind_rows(results) %>%
  arrange(rmse_C, mae_C)

readr::write_csv(res, OUT_CSV)

valid <- res %>% filter(!is.na(rmse_C), !is.na(distance_to_nearest_km), !is.na(abs_delta_altitude_m))

safe_cor <- function(x, y, method = "spearman") {
  ok <- is.finite(x) & is.finite(y)
  if (sum(ok) < 5) return(NA_real_)
  suppressWarnings(cor(x[ok], y[ok], method = method))
}

corr_dist_rmse <- safe_cor(valid$distance_to_nearest_km, valid$rmse_C)
corr_alt_rmse <- safe_cor(valid$abs_delta_altitude_m, valid$rmse_C)
corr_dist_cor <- safe_cor(valid$distance_to_nearest_km, valid$cor)
corr_alt_cor <- safe_cor(valid$abs_delta_altitude_m, valid$cor)

rel_tbl <- tibble(
  metric = c(
    "spearman(distance_km, rmse_C)",
    "spearman(abs_delta_altitude_m, rmse_C)",
    "spearman(distance_km, cor)",
    "spearman(abs_delta_altitude_m, cor)"
  ),
  value = c(corr_dist_rmse, corr_alt_rmse, corr_dist_cor, corr_alt_cor)
)
readr::write_csv(rel_tbl, OUT_REL_CSV)

png(filename = OUT_PNG, width = 1300, height = 700, res = 120)
op <- par(no.readonly = TRUE)
on.exit({ par(op); dev.off() }, add = TRUE)
par(mfrow = c(1, 2), mar = c(5, 5, 4, 1) + 0.1)

plot(
  valid$distance_to_nearest_km,
  valid$rmse_C,
  pch = 19,
  col = "#1F77B4AA",
  xlab = "Distanz zur nächsten Station [km]",
  ylab = "RMSE [°C]",
  main = "Simulationsgüte vs Distanz"
)
if (nrow(valid) >= 3) {
  fit1 <- lm(rmse_C ~ distance_to_nearest_km, data = valid)
  abline(fit1, col = "#D62728", lwd = 2)
}

plot(
  valid$abs_delta_altitude_m,
  valid$rmse_C,
  pch = 19,
  col = "#2CA02CAA",
  xlab = "|Höhenunterschied| zur nächsten Station [m]",
  ylab = "RMSE [°C]",
  main = "Simulationsgüte vs Höhendifferenz"
)
if (nrow(valid) >= 3) {
  fit2 <- lm(rmse_C ~ abs_delta_altitude_m, data = valid)
  abline(fit2, col = "#D62728", lwd = 2)
}

summary_lines <- c(
  sprintf("Zeitraum: %s bis %s", START_DATE, END_DATE),
  sprintf("Stationen gesamt: %d", nrow(res)),
  sprintf("Stationen mit ausreichender Überlappung: %d", nrow(valid)),
  "",
  "Ratings:",
  capture.output(print(res %>% count(quality_rating, sort = TRUE), n = Inf)),
  "",
  "Zusammenhang Distanz/Höhe mit Simulationsgüte:",
  sprintf("- Spearman(distance, RMSE): %.3f", corr_dist_rmse),
  sprintf("- Spearman(|delta_h|, RMSE): %.3f", corr_alt_rmse),
  sprintf("- Spearman(distance, Correlation): %.3f", corr_dist_cor),
  sprintf("- Spearman(|delta_h|, Correlation): %.3f", corr_alt_cor),
  "",
  "Schlechteste 10 Stationen nach RMSE:",
  capture.output(print(res %>% arrange(desc(rmse_C)) %>% select(target_station_id, nearest_station_id, distance_to_nearest_km, delta_altitude_m, rmse_C, mae_C, cor, quality_rating) %>% slice_head(n = 10), n = Inf)),
  "",
  "Beste 10 Stationen nach RMSE:",
  capture.output(print(res %>% arrange(rmse_C) %>% select(target_station_id, nearest_station_id, distance_to_nearest_km, delta_altitude_m, rmse_C, mae_C, cor, quality_rating) %>% slice_head(n = 10), n = Inf))
)
writeLines(summary_lines, OUT_SUMMARY)

message("\nFertig.")
message("- Stationsbewertung: ", OUT_CSV)
message("- Distanz/Höhen-Zusammenhang: ", OUT_REL_CSV)
message("- Zusammenfassung: ", OUT_SUMMARY)
message("- Scatterplot: ", OUT_PNG)
