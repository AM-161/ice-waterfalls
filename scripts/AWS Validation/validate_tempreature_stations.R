#!/usr/bin/env Rscript

# ============================================================
# validate_tempreature_stations.R
# ------------------------------------------------------------
# Script purpose:
# - Validates how well temperature at a target AWS station can be
#   simulated from selected comparison stations via lapse-rate adjustment.
# - Downloads temperature time series for:
#   (1) target station,
#   (2) nearest station,
#   (3) inversion stations (38, IMUT2, IMUT1).
# - Writes ranked validation metrics and a temperature/difference plot.
#
# Outputs are written to: scripts/AWS Validation/
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(readr)
  library(lubridate)
  library(httr2)
  library(jsonlite)
})

TZ_LOCAL <- "Europe/Vienna"
LAPSE_K_PER_M <- 0.0065
PATH_STATIONS <- "data/AWS/stations_all.csv"
PATH_INV_DIR <- "data/_cache_inversion"
OUT_CSV <- "scripts/AWS Validation/validation_temperature_stations.csv"
OUT_PNG <- "scripts/AWS Validation/validation_temperature_stations.png"
TARGET_STATION_DEFAULT <- "SLSE1"

args <- commandArgs(trailingOnly = TRUE)
START_DATE <- if (length(args) >= 1) as.Date(args[[1]]) else Sys.Date() - 30
END_DATE   <- if (length(args) >= 2) as.Date(args[[2]]) else Sys.Date() - 1
TARGET_STATION <- if (length(args) >= 3) as.character(args[[3]]) else TARGET_STATION_DEFAULT
PATH_INV_RDS <- file.path(PATH_INV_DIR, sprintf("inversion_%s.rds", format(END_DATE, "%Y%m%d")))

if (is.na(START_DATE) || is.na(END_DATE) || START_DATE > END_DATE) {
  stop("Ungültiger Zeitraum. Nutzung: Rscript \"scripts/AWS Validation/validate_tempreature_stations.R\" YYYY-MM-DD YYYY-MM-DD [STATION_ID]")
}
if (is.na(TARGET_STATION) || !nzchar(TARGET_STATION)) {
  stop("Ungültige Zielstation. Bitte STATION_ID als 3. Argument übergeben.")
}


OUT_DIR <- dirname(OUT_CSV)
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
    transmute(
      timestamp = as.POSIXct(timestamp, tz = TZ_LOCAL),
      TL = to_num(value)
    ) %>%
    arrange(timestamp) %>%
    filter(timestamp >= as.POSIXct(start_date, tz = TZ_LOCAL),
           timestamp < as.POSIXct(end_date + 1, tz = TZ_LOCAL))
}

get_geosphere_station_tl <- function(start_date, end_date, station_id) {
  base_url <- "https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-10min"
  start_q <- sprintf("%sT00:00", as.character(as.Date(start_date)))
  end_q <- sprintf("%sT23:50", as.character(as.Date(end_date)))

  param_tries <- c("tl", "TL")

  for (p in param_tries) {
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
      cand <- c(name, tolower(name), toupper(name))
      for (nm in cand) {
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

    return(tibble(
      timestamp = with_tz(time_utc, TZ_LOCAL),
      TL = pull_anycase("tl")
    ))
  }

  tibble(timestamp = as.POSIXct(character(), tz = TZ_LOCAL), TL = numeric())
}

get_station_tl <- function(start_date, end_date, station_id, source) {
  if (identical(source, "GeoSphere")) return(get_geosphere_station_tl(start_date, end_date, station_id))
  if (identical(source, "LWD")) return(get_lwd_station_tl(start_date, end_date, station_id))
  tibble(timestamp = as.POSIXct(character(), tz = TZ_LOCAL), TL = numeric())
}

stations_all <- readr::read_csv(PATH_STATIONS, show_col_types = FALSE) %>%
  mutate(
    station_id = as.character(station_id),
    source = as.character(source),
    lon = to_num(lon),
    lat = to_num(lat),
    altitude_m = to_num(altitude_m),
    tl = as.logical(tl)
  ) %>%
  filter(!is.na(station_id), !is.na(source), !is.na(altitude_m), tl)

target_meta <- stations_all %>% filter(station_id == TARGET_STATION) %>% slice(1)
if (nrow(target_meta) == 0) stop("Station ", TARGET_STATION, " nicht in stations_all gefunden.")

target_alt <- target_meta$altitude_m[[1]]
target_lon <- target_meta$lon[[1]]
target_lat <- target_meta$lat[[1]]

message("Simulationsformel: Standard-Lapse + optional Inversionsprofil (wenn inv_active)")
message("Zielstation für Vergleich: ", TARGET_STATION,
        " (", target_meta$source[[1]], ", ", target_alt, " m)")

deg2rad <- function(x) x * pi / 180
haversine_km <- function(lon1, lat1, lon2, lat2) {
  r <- 6371
  dlon <- deg2rad(lon2 - lon1)
  dlat <- deg2rad(lat2 - lat1)
  a <- sin(dlat / 2)^2 + cos(deg2rad(lat1)) * cos(deg2rad(lat2)) * sin(dlon / 2)^2
  2 * r * atan2(sqrt(a), sqrt(1 - a))
}

nearest_station <- stations_all %>%
  filter(
    station_id != TARGET_STATION,
    is.finite(lon), is.finite(lat),
    is.finite(target_lon), is.finite(target_lat)
  ) %>%
  mutate(dist_to_target_km = haversine_km(lon, lat, target_lon, target_lat)) %>%
  arrange(dist_to_target_km) %>%
  slice(1) %>%
  transmute(station_id, source, altitude_m, reason = "nearest")

inversion_stations <- tibble(
  station_id = c("38", "IMUT2", "IMUT1"),
  source = c("GeoSphere", "LWD", "LWD"),
  altitude_m = c(860, 1935, 2580),
  reason = "inversion"
)

candidates <- bind_rows(
  tibble(
    station_id = TARGET_STATION,
    source = target_meta$source[[1]],
    altitude_m = target_alt,
    reason = "target"
  ),
  nearest_station,
  inversion_stations
) %>%
  distinct(station_id, .keep_all = TRUE)

message("Vergleich nur mit benötigten Stationen: ", paste(candidates$station_id, collapse = ", "))

message("Lade Referenzdaten von ", TARGET_STATION, " (", target_meta$source[[1]], ") ...")
ref <- get_station_tl(START_DATE, END_DATE, TARGET_STATION, target_meta$source[[1]]) %>%
  rename(TL_ref = TL) %>%
  filter(!is.na(TL_ref))

if (nrow(ref) == 0) stop("Keine Temperaturdaten für Zielstation ", TARGET_STATION)

if (file.exists(PATH_INV_RDS)) {
  inv <- readRDS(PATH_INV_RDS) %>%
    mutate(
      timestamp = as.POSIXct(time, tz = TZ_LOCAL),
      inv_active = ifelse(is.na(inv_active), FALSE, inv_active),
      grad01_K_per_m = as.numeric(grad01_K_per_m),
      grad12_K_per_m = as.numeric(grad12_K_per_m)
    ) %>%
    select(timestamp, inv_active, grad01_K_per_m, grad12_K_per_m)
  message("Inversion cache geladen: ", PATH_INV_RDS)
} else {
  inv <- tibble(
    timestamp = as.POSIXct(character(), tz = TZ_LOCAL),
    inv_active = logical(),
    grad01_K_per_m = numeric(),
    grad12_K_per_m = numeric()
  )
  message("⚠️ Inversion cache fehlt: ", PATH_INV_RDS, " (Fallback auf konstante Lapse-Rate)")
}

results <- vector("list", nrow(candidates))
cmp_by_station <- vector("list", nrow(candidates))

for (i in seq_len(nrow(candidates))) {
  st <- candidates[i, ]
  sid <- st$station_id[[1]]
  src <- st$source[[1]]
  z_st <- st$altitude_m[[1]]

  message(sprintf("[%d/%d] %s (%s, %s)", i, nrow(candidates), sid, src, st$reason[[1]]))

  dat <- get_station_tl(START_DATE, END_DATE, sid, src)
  if (nrow(dat) == 0) next

  cmp <- dat %>%
    filter(!is.na(TL)) %>%
    transmute(timestamp, TL = as.numeric(TL)) %>%
    left_join(inv, by = "timestamp") %>%
    mutate(
      inv_active = ifelse(is.na(inv_active), FALSE, inv_active),
      use_prof = inv_active & is.finite(grad01_K_per_m) & is.finite(grad12_K_per_m),
      TL_lapse = TL - LAPSE_K_PER_M * (target_alt - z_st),
      TL_sim = if_else(
        use_prof,
        {
          z1_m <- 1935
          lo <- pmin(z_st, target_alt)
          hi <- pmax(z_st, target_alt)
          len_low  <- pmax(0, pmin(hi, z1_m) - lo)
          len_high <- pmax(0, hi - pmax(lo, z1_m))
          sgn <- if_else(target_alt >= z_st, 1, -1)
          TL + sgn * (grad01_K_per_m * len_low + grad12_K_per_m * len_high)
        },
        TL_lapse
      )
    ) %>%
    inner_join(ref, by = "timestamp") %>%
    mutate(diff_C = TL_sim - TL_ref)

  if (nrow(cmp) < 30) next

  err <- cmp$TL_sim - cmp$TL_ref
  mae <- mean(abs(err), na.rm = TRUE)
  rmse <- sqrt(mean(err^2, na.rm = TRUE))
  bias <- mean(err, na.rm = TRUE)
  corv <- suppressWarnings(cor(cmp$TL_sim, cmp$TL_ref, use = "complete.obs"))

  results[[i]] <- tibble(
    station_id = sid,
    source = src,
    reason = st$reason[[1]],
    altitude_m = z_st,
    n = nrow(cmp),
    mae_C = mae,
    rmse_C = rmse,
    bias_C = bias,
    cor = corv,
    inv_share = mean(cmp$use_prof, na.rm = TRUE)
  )

  cmp_by_station[[i]] <- cmp %>%
    transmute(
      station_id = sid,
      timestamp,
      TL_sim,
      TL_ref,
      diff_C
    )
}

res <- bind_rows(results) %>%
  arrange(rmse_C, mae_C)

if (nrow(res) == 0) stop("Keine Station mit ausreichender Datenüberlappung gefunden.")

readr::write_csv(res, OUT_CSV)

cmp_all <- bind_rows(cmp_by_station)

if (nrow(cmp_all) > 0) {
  # Für das Diagramm bevorzugen wir die nächste Station (reason="nearest").
  # Falls diese keine ausreichenden Daten hat, fallback auf beste non-target Station.
  nearest_row <- res %>% filter(reason == "nearest") %>% slice(1)
  res_non_target <- res %>% filter(station_id != TARGET_STATION)
  best_non_target <- res_non_target %>% slice(1)

  if (nrow(nearest_row) == 1) {
    plot_station <- nearest_row$station_id[[1]]
    plot_row <- nearest_row
  } else if (nrow(best_non_target) == 1) {
    plot_station <- best_non_target$station_id[[1]]
    plot_row <- best_non_target
  } else {
    plot_station <- res$station_id[[1]]
    plot_row <- res %>% slice(1)
  }

  cmp_plot <- cmp_all %>% filter(station_id == plot_station) %>% arrange(timestamp)

  if (identical(plot_station, TARGET_STATION)) {
    message("⚠️ Nur Zielstation selbst verfügbar; Vergleich ist Baseline-nahe 0.")
  }

  message(sprintf(
    "Plot verwendet Station %s (reason=%s, n=%d, RMSE=%.2f, MAE=%.2f, Bias=%.2f)",
    plot_row$station_id[[1]], plot_row$reason[[1]], plot_row$n[[1]],
    plot_row$rmse_C[[1]], plot_row$mae_C[[1]], plot_row$bias_C[[1]]
  ))

  png(filename = OUT_PNG, width = 1300, height = 800, res = 120)
  op <- par(no.readonly = TRUE)
  on.exit({ par(op); dev.off() }, add = TRUE)

  layout(matrix(c(1, 2), nrow = 2), heights = c(2, 1))

  # Panel 1: simulierte vs. reale Temperatur
  par(mar = c(3, 5, 4, 2) + 0.1)
  y_rng <- range(c(cmp_plot$TL_sim, cmp_plot$TL_ref), na.rm = TRUE)
  plot(
    cmp_plot$timestamp,
    cmp_plot$TL_ref,
    type = "l",
    col = "#111111",
    lwd = 2,
    ylim = y_rng,
    xlab = "",
    ylab = "Temperatur [°C]",
    main = paste0(
      "Temperaturvergleich zu ", TARGET_STATION,
      " (Vergleichsstation: ", plot_station, ")"
    )
  )
  lines(cmp_plot$timestamp, cmp_plot$TL_sim, col = "#1F77B4", lwd = 2)
  legend(
    "topright",
    legend = c("Real (Zielstation)", "Simuliert (aus Vergleichsstation)"),
    col = c("#111111", "#1F77B4"),
    lwd = 2,
    bty = "n"
  )

  # Panel 2: Differenz Sim - Real
  par(mar = c(5, 5, 2, 2) + 0.1)
  plot(
    cmp_plot$timestamp,
    cmp_plot$diff_C,
    type = "l",
    col = "#D62728",
    lwd = 1.8,
    xlab = "Zeit",
    ylab = "Differenz [°C]",
    main = "Abweichung (Simuliert - Real)"
  )
  abline(h = 0, col = "#666666", lty = 2)
  legend(
    "topright",
    legend = c(
      paste0("MAE: ", round(plot_row$mae_C[[1]], 2), " °C"),
      paste0("RMSE: ", round(plot_row$rmse_C[[1]], 2), " °C"),
      paste0("Bias: ", round(plot_row$bias_C[[1]], 2), " °C")
    ),
    bty = "n"
  )
}

message("\nFertig. Vergleich gespeichert in: ", OUT_CSV)
if (file.exists(OUT_PNG)) {
  message("Temperatur-/Differenz-Diagramm gespeichert in: ", OUT_PNG)
} else {
  message("⚠️ Kein Differenz-Diagramm erzeugt (keine Vergleichsdaten).")
}
message("Zeitraum: ", START_DATE, " bis ", END_DATE)
message("Ergebnisse (inkl. Baseline der Zielstation):")
print(res, n = nrow(res))

slse1_self <- res %>% filter(station_id == TARGET_STATION)
if (nrow(slse1_self) == 1) {
  message("\nSLSE1 gegen sich selbst (Baseline):")
  print(slse1_self)
}
