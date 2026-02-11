# =====================================================================
# Urheberrechtlicher Hinweis / Nutzungseinsschränkung
# ---------------------------------------------------------------------
# © 2025 Alle Rechte vorbehalten.
#
# Dieses Skript wurde im Rahmen der Masterarbeit zur Kletterbarkeit
# von Eisfällen in Nordtirol erstellt. Der Code darf ohne vorherige
# ausdrückliche schriftliche Zustimmung des Autors weder ganz noch
# teilweise kopiert, verändert, weiterverbreitet oder in eigene
# Projekte (insbesondere kommerzielle Anwendungen oder proprietäre
# Software) integriert werden.
#
# Eine Nutzung über das reine Lesen und Nachvollziehen im Kontext
# dieser Masterarbeit hinaus ist nicht gestattet. Bei Rückfragen
# oder einer gewünschten Nutzung bitte vorher Kontakt mit dem Autor
# aufnehmen.
# =====================================================================

suppressPackageStartupMessages({
  library(httr)
  library(raster)
  library(leaflet)
  library(htmlwidgets)
  library(htmltools)
  library(ncdf4)
  library(readr)
  library(dplyr)
  library(tibble)
  library(lubridate)
  library(png)
})

# 0) Zeitraum & Gebiet -------------------------------------------------

start_all    <- as.Date("2025-10-01") # Startdatum der Saison
end_all      <- Sys.Date()           # bis heute
chunk_days   <- 2                    # 2-Tages-Chunks (API-Limit)
chunk_starts <- seq.Date(start_all, end_all, by = chunk_days)

# Nordtirol-BBox in WGS84
bbox <- c(
  46.7,  # lat_min
  10.1,  # lon_min
  47.7,  # lat_max
  12.2   # lon_max
)

# INCA-Parameter (erweitert für Wind, Feuchte etc.)
parameters    <- c("RR", "T2M", "RH2M", "UU", "VV", "GL", "P0", "TD2M")
param_str     <- paste(parameters, collapse = ",")
base_url_inca <- "https://dataset.api.hub.geosphere.at/v1/grid/historical/inca-v1-1h-1km"

out_dir <- "data/inca_nordtirol"
out_dir_nwp <- "data/nwp_2500m_forecast"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(out_dir_nwp, showWarnings = FALSE, recursive = TRUE)

nc_files <- character(length(chunk_starts))

# 1) INCA-Daten chunkweise laden (existierende Dateien überspringen) ---

for (idx in seq_along(chunk_starts)) {
  cs <- as.Date(chunk_starts[idx])
  ce <- as.Date(min(cs + (chunk_days - 1), end_all))
  
  message("Chunk: ", cs, " bis ", ce)
  
  start_time <- sprintf("%sT00:00", format(cs, "%Y-%m-%d"))
  end_time   <- sprintf("%sT23:00", format(ce, "%Y-%m-%d"))
  
  outfile <- file.path(
    out_dir,
    sprintf("inca_nordtirol_%s_%s.nc",
            format(cs, "%Y%m%d"),
            format(ce, "%Y%m%d"))
  )
  
  if (file.exists(outfile) && file.info(outfile)$size > 0) {
    message("  -> übersprungen (existiert): ", outfile)
  } else {
    query <- list(
      parameters    = param_str,
      start         = start_time,
      end           = end_time,
      bbox          = paste(bbox, collapse = ","),
      output_format = "netcdf",
      filename      = "inca_nordtirol"
    )
    
    resp <- GET(
      url   = base_url_inca,
      query = query,
      write_disk(outfile, overwrite = TRUE)
    )
    stop_for_status(resp)
    
    message("  -> gespeichert: ", outfile, " (", file.info(outfile)$size, " Bytes)")
  }
  
  nc_files[idx] <- outfile
}

nc_files <- sort(unique(nc_files))

# 2) Alle NetCDFs einlesen und in ein Objekt packen --------------------

convert_nc_time <- function(time_vals, time_units) {
  if (!grepl("since", time_units)) {
    origin <- as.POSIXct("1970-01-01 00:00:00", tz = "UTC")
    return(origin + time_vals * 3600)
  }
  unit_str   <- sub(" since.*", "", time_units)
  origin_str <- sub(".*since ", "", time_units)
  origin     <- as.POSIXct(origin_str, tz = "UTC")
  
  if (grepl("hour", unit_str, ignore.case = TRUE)) {
    origin + time_vals * 3600
  } else if (grepl("second", unit_str, ignore.case = TRUE)) {
    origin + time_vals
  } else if (grepl("day", unit_str, ignore.case = TRUE)) {
    origin + time_vals * 86400
  } else {
    origin + time_vals
  }
}

vars_wanted <- c("T2M", "RH2M", "RR", "GL", "UU", "VV", "TD2M", "P0")

nc0       <- nc_open(nc_files[1])
varnames0 <- names(nc0$var)

lon_name <- intersect(c("lon", "longitude", "x", "xc"), varnames0)[1]
lat_name <- intersect(c("lat", "latitude", "y", "yc"), varnames0)[1]

lon0 <- ncvar_get(nc0, lon_name)  # [nx, ny]
lat0 <- ncvar_get(nc0, lat_name)  # [nx, ny]

nx <- dim(lon0)[1]
ny <- dim(lon0)[2]

time_dim_name <- names(nc0$dim)[grep("time", tolower(names(nc0$dim)))[1]]
vars_found    <- intersect(vars_wanted, varnames0)
nc_close(nc0)

# Zeitlängen pro Datei
nt_per_file <- integer(length(nc_files))
for (i in seq_along(nc_files)) {
  nc <- nc_open(nc_files[i])
  nt_per_file[i] <- length(ncvar_get(nc, time_dim_name))
  nc_close(nc)
}
nt_total <- sum(nt_per_file)

# Arrays allozieren

time_all  <- as.POSIXct(rep(NA_real_, nt_total), origin = "1970-01-01", tz = "UTC")
inca_data <- lapply(vars_found, function(.) array(NA_real_, dim = c(nx, ny, nt_total)))
names(inca_data) <- vars_found

# Daten einlesen
pos <- 1L
for (i in seq_along(nc_files)) {
  nc <- nc_open(nc_files[i])
  
  time_vals  <- ncvar_get(nc, time_dim_name)
  time_units <- ncatt_get(nc, time_dim_name, "units")$value
  nt_i       <- length(time_vals)
  
  idx <- pos:(pos + nt_i - 1L)
  time_all[idx] <- convert_nc_time(time_vals, time_units)
  
  for (v in vars_found) {
    inca_data[[v]][ , , idx] <- ncvar_get(nc, v)
  }
  
  nc_close(nc)
  pos <- pos + nt_i
}

inca_nordtirol_all <- list(
  lon  = lon0,
  lat  = lat0,
  time = time_all,
  data = inca_data
)

# 3) FDH/MDH (stundenweise) + Orientierung + Raster-Template -----------

T2M_arr <- inca_nordtirol_all$data$T2M   # °C [nx, ny, nt]
RH_arr  <- inca_nordtirol_all$data$RH2M  # %  [nx, ny, nt]
UU_arr  <- inca_nordtirol_all$data$UU    # m/s
VV_arr  <- inca_nordtirol_all$data$VV    # m/s

lon <- inca_nordtirol_all$lon
lat <- inca_nordtirol_all$lat

nx <- dim(T2M_arr)[1]
ny <- dim(T2M_arr)[2]
nt <- dim(T2M_arr)[3]

# Orientierung (S->N, W->E)
lat_mean_j <- colMeans(lat, na.rm = TRUE)
if (lat_mean_j[1] < tail(lat_mean_j, 1)) {
  idx_j <- ny:1
  lon   <- lon[, idx_j]
  lat   <- lat[, idx_j]
  T2M_arr <- T2M_arr[, idx_j, , drop = FALSE]
  RH_arr  <- RH_arr[,  idx_j, , drop = FALSE]
  UU_arr  <- UU_arr[,  idx_j, , drop = FALSE]
  VV_arr  <- VV_arr[,  idx_j, , drop = FALSE]
}

lon_mean_i <- rowMeans(lon, na.rm = TRUE)
if (lon_mean_i[1] > tail(lon_mean_i, 1)) {
  idx_i <- nx:1
  lon   <- lon[idx_i, ]
  lat   <- lat[idx_i, ]
  T2M_arr <- T2M_arr[idx_i, , , drop = FALSE]
  RH_arr  <- RH_arr[idx_i, , , drop = FALSE]
  UU_arr  <- UU_arr[idx_i, , , drop = FALSE]
  VV_arr  <- VV_arr[idx_i, , , drop = FALSE]
}

nx <- dim(T2M_arr)[1]
ny <- dim(T2M_arr)[2]
nt <- dim(T2M_arr)[3]

FDH_hourly <- pmax(-T2M_arr, 0)
MDH_hourly <- pmax( T2M_arr, 0)
W_arr      <- sqrt(UU_arr^2 + VV_arr^2)

wind_ref <- mean(W_arr, na.rm = TRUE)
if (!is.finite(wind_ref) || wind_ref <= 0) wind_ref <- 5

FDH_mat_plain <- t(apply(FDH_hourly, c(1, 2), sum, na.rm = TRUE)) # [ny, nx]

r_template <- raster(FDH_mat_plain)
extent(r_template) <- c(min(lon, na.rm = TRUE),
                        max(lon, na.rm = TRUE),
                        min(lat, na.rm = TRUE),
                        max(lat, na.rm = TRUE))
crs(r_template) <- "EPSG:4326"

# 4) DEM auf INCA-Grid + Expositionsindex ------------------------------

dem_inca <- raster("data/DEM/DEM_Tirol_INCAgrid_1km_epsg4326.tif")
crs(dem_inca) <- "EPSG:4326"
names(dem_inca) <- "elev_m"

sl_as       <- terrain(dem_inca, opt = c("slope", "aspect"), unit = "degrees")
aspect_inca <- sl_as[["aspect"]]

aspect_rad_from_north <- (aspect_inca - 90) * pi / 180
northness_r           <- cos(aspect_rad_from_north)

solar_index_r  <- (1 - northness_r) / 2
solar_index_r[is.na(solar_index_r[])] <- 0.5
solar_index_ij <- t(as.matrix(solar_index_r))

# --- Höhen-Gewichte: Zellen > 2500 m werden weniger gewichtet --------
alt_threshold    <- 2500
alt_weight_high  <- 0.5

alt_weight_r <- raster::calc(dem_inca, fun = function(z) {
  ifelse(is.na(z), NA,
         ifelse(z > alt_threshold, alt_weight_high, 1))
})
names(alt_weight_r) <- "w_alt"

weighted_mean_alt <- function(r, w_rast = alt_weight_r) {
  v <- raster::getValues(r)
  w <- raster::getValues(w_rast)
  ok <- is.finite(v) & is.finite(w)
  if (!any(ok)) return(NA_real_)
  sum(v[ok] * w[ok]) / sum(w[ok])
}

# 5) Zeitabhängige Sonnenhöhe + Zeit-Gewichtung ------------------------

time_vec <- inca_nordtirol_all$time
if (length(time_vec) != nt) {
  if (length(time_vec) > nt) time_vec <- tail(time_vec, nt)
  else stop("Länge von time_vec (", length(time_vec), ") != nt (", nt, ")")
}

time_local <- as.POSIXlt(time_vec, tz = "Europe/Vienna")

doy  <- time_local$yday + 1
hour <- time_local$hour + time_local$min / 60 + time_local$sec / 3600

delta_t <- 23.44 * pi/180 * sin(2 * pi * (284 + doy) / 365)

lat_center_deg <- (min(lat, na.rm = TRUE) + max(lat, na.rm = TRUE)) / 2
lat_center_rad <- lat_center_deg * pi / 180

H_t <- (hour - 12) * 15 * pi / 180

sin_alpha_t <- sin(lat_center_rad) * sin(delta_t) +
  cos(lat_center_rad) * cos(delta_t) * cos(H_t)

sin_alpha_t[sin_alpha_t < 0] <- 0
solar_height_factor_t <- sin_alpha_t

max_time <- max(time_vec, na.rm = TRUE)
age_days <- as.numeric(difftime(max_time, time_vec, units = "days"))

tau_days      <- 10
weight_time_t <- exp(-age_days / tau_days)

t_start           <- min(time_vec, na.rm = TRUE)
time_offset_hours <- as.numeric(difftime(time_vec, t_start, units = "hours"))

# Lokalzeit für Hist-Snapshots

time_local_all <- as.POSIXct(format(time_vec, tz = "Europe/Vienna", usetz = TRUE),
                             tz = "Europe/Vienna")

t_min_local <- min(time_local_all, na.rm = TRUE)
t_max_local <- max(time_local_all, na.rm = TRUE)

t0_local <- as.POSIXct(paste0(format(t_min_local, "%Y-%m-%d"), " 07:00:00"),
                       tz = "Europe/Vienna")
if (t0_local < t_min_local) t0_local <- t0_local + 24 * 3600

t_last_local <- as.POSIXct(paste0(format(t_max_local, "%Y-%m-%d"), " 07:00:00"),
                           tz = "Europe/Vienna")
if (t_last_local > t_max_local) t_last_local <- t_last_local - 24 * 3600

if (t_last_local < t0_local) {
  snap_hours_hist <- numeric(0)
} else {
  snap_times_local <- seq(from = t0_local, to = t_last_local, by = "1 day")
  snap_times_utc   <- as.POSIXct(format(snap_times_local, tz = "UTC", usetz = TRUE),
                                 tz = "UTC")
  snap_hours_hist  <- as.numeric(difftime(snap_times_utc, t_start, units = "hours"))
}

FDH_hist_layers <- list()
FDH_hist_labels <- character(0)
FDH_hist_times  <- as.POSIXct(character(0), tz = "UTC")
snap_idx_hist   <- 1L

# 6) Effektive FDH (FDHm + Wind + Feuchte + Strahlung + Zeit) ---------

k_expo   <- 0.5
k_wind   <- 0.5
wind_min <- 0.5
wind_max <- 2.0
rh_opt   <- 0.7
rh_sig   <- 0.15
k_melt   <- 1.2

FDH_sum_eff <- matrix(0, nrow = nx, ncol = ny)

for (k in seq_len(nt)) {
  T_k  <- T2M_arr[ , , k]
  RH_k <- RH_arr[ , , k] / 100
  
  if (all(is.na(T_k)) || all(is.na(RH_k))) {
    t_k_hours <- time_offset_hours[k]
    while (snap_idx_hist <= length(snap_hours_hist) &&
           t_k_hours >= snap_hours_hist[snap_idx_hist] - 0.5) {
      
      FDH_k_snap <- FDH_sum_eff
      FDH_k_snap[FDH_k_snap < 0] <- 0
      
      r_FDH_snap <- raster(t(FDH_k_snap))
      extent(r_FDH_snap) <- extent(r_template)
      crs(r_FDH_snap)    <- crs(r_template)
      
      label_time <- t_start + snap_hours_hist[snap_idx_hist] * 3600
      label_str  <- format(label_time, "%d.%m.%Y")
      
      FDH_hist_layers[[length(FDH_hist_layers) + 1L]] <- r_FDH_snap
      FDH_hist_labels <- c(FDH_hist_labels, label_str)
      FDH_hist_times  <- c(FDH_hist_times, label_time)
      
      snap_idx_hist <- snap_idx_hist + 1L
    }
    next
  }
  
  RH_k_eff <- RH_k
  RH_k_eff[is.na(RH_k_eff)] <- rh_opt
  
  FDH_k <- pmax(-T_k, 0)
  MDH_k <- pmax( T_k, 0)
  W_k   <- W_arr[ , , k]
  
  wind_norm <- (W_k - wind_ref) / wind_ref
  f_wind    <- 1 + k_wind * wind_norm
  f_wind[!is.finite(f_wind)] <- 1
  f_wind <- pmax(wind_min, pmin(wind_max, f_wind))
  
  f_rh_peak <- exp(- (RH_k_eff - rh_opt)^2 / (2 * rh_sig^2))
  f_rh      <- 0.5 + 0.5 * f_rh_peak
  
  s_height <- solar_height_factor_t[k]
  if (s_height == 0) {
    f_rad <- 1
  } else {
    f_rad <- 1 - k_expo * s_height * solar_index_ij
  }
  f_rad[!is.finite(f_rad)] <- 1
  f_rad <- pmax(0, pmin(1, f_rad))
  
  f_time <- weight_time_t[k]
  if (!is.finite(f_time)) f_time <- 1
  
  FDH_eff_k <- FDH_k * f_wind * f_rh * f_rad * f_time
  MDH_eff_k <- MDH_k * k_melt * f_time
  
  FDH_sum_eff <- FDH_sum_eff + (FDH_eff_k - MDH_eff_k)
  
  t_k_hours <- time_offset_hours[k]
  while (snap_idx_hist <= length(snap_hours_hist) &&
         t_k_hours >= snap_hours_hist[snap_idx_hist] - 0.5) {
    
    FDH_k_snap <- FDH_sum_eff
    FDH_k_snap[FDH_k_snap < 0] <- 0
    
    r_FDH_snap <- raster(t(FDH_k_snap))
    extent(r_FDH_snap) <- extent(r_template)
    crs(r_FDH_snap)    <- crs(r_template)
    
    label_time <- t_start + snap_hours_hist[snap_idx_hist] * 3600
    label_str  <- format(label_time, "%d.%m.%Y")
    
    FDH_hist_layers[[length(FDH_hist_layers) + 1L]] <- r_FDH_snap
    FDH_hist_labels <- c(FDH_hist_labels, label_str)
    FDH_hist_times  <- c(FDH_hist_times, label_time)
    
    snap_idx_hist <- snap_idx_hist + 1L
  }
}

FDH_sum_eff[FDH_sum_eff < 0] <- 0

r_FDH_eff <- raster(t(FDH_sum_eff))
extent(r_FDH_eff) <- extent(r_template)
crs(r_FDH_eff)    <- crs(r_template)
names(r_FDH_eff)  <- "FDH_eff_C_h"

# 7) Effektive FDH -> Eisdicke + Climbability-Index (aktuell) ----------

h_c      <- 30
rho_i    <- 880
Lf       <- 334000
FDH_crit <- 1

alpha <- h_c * 3600 / (rho_i * Lf)

score_h_fun <- function(h, h_min, h_opt) {
  s <- (h - h_min) / (h_opt - h_min)
  s[h <= h_min] <- 0
  s[h >= h_opt] <- 1
  s[!is.finite(s)] <- 0
  s
}

score_T_fun <- function(Tv, T_opt, T_min, T_max, range_T) {
  s <- 1 - abs(Tv - T_opt) / range_T
  s[Tv <= T_min | Tv >= T_max] <- 0
  s[s < 0] <- 0
  s[!is.finite(s)] <- 0
  s
}

score_RH_fun <- function(rh, RH_opt, RH_sig) {
  peak <- exp(- (rh - RH_opt)^2 / (2 * RH_sig^2))
  s <- peak
  s[!is.finite(s)] <- 0
  s
}

ice_hist_layers <- list()
ice_hist_labels <- character(0)

if (length(FDH_hist_layers) > 0) {
  ice_hist_layers <- vector("list", length(FDH_hist_layers))
  ice_hist_labels <- FDH_hist_labels
  for (i in seq_along(FDH_hist_layers)) {
    r_fd  <- FDH_hist_layers[[i]]
    ice_i <- r_fd * alpha
    ice_i[r_fd < FDH_crit] <- NA
    names(ice_i) <- paste0("h_pot_expo_hist_", ice_hist_labels[i])
    ice_hist_layers[[i]] <- ice_i
  }
}

ice_thick_expo <- r_FDH_eff * alpha
ice_thick_expo[r_FDH_eff < FDH_crit] <- NA
names(ice_thick_expo) <- "h_pot_expo_m"

h_mat_ij <- FDH_sum_eff * alpha

T_last_ij  <- T2M_arr[ , , nt]
RH_last_ij <- RH_arr[ , , nt] / 100

n_tr3   <- min(72, nt)
idx_tr3 <- (nt - n_tr3 + 1):nt

Tr3_ij <- apply(T2M_arr[ , , idx_tr3, drop = FALSE], c(1, 2), mean, na.rm = TRUE)

h_min <- 0.10
h_opt <- 0.50
score_h <- score_h_fun(h_mat_ij, h_min, h_opt)

T_opt   <- -4
T_min   <- -20
T_max   <- 0
range_T <- max(T_opt - T_min, T_max - T_opt)
score_T <- score_T_fun(T_last_ij, T_opt, T_min, T_max, range_T)

T3_opt   <- -6
T3_min   <- -20
T3_max   <- -1
range_T3 <- max(T3_opt - T3_min, T3_max - T3_opt)
score_T3 <- score_T_fun(Tr3_ij, T3_opt, T3_min, T3_max, range_T3)

RH_opt_c <- 0.7
RH_sig_c <- 0.2
score_RH <- score_RH_fun(RH_last_ij, RH_opt_c, RH_sig_c)

# Climbability-Historie (wie bei dir)
climb_hist_layers <- list()
climb_hist_labels <- character(0)

if (length(FDH_hist_layers) > 0 && length(FDH_hist_labels) == length(FDH_hist_layers)) {
  climb_hist_layers <- vector("list", length(FDH_hist_layers))
  climb_hist_labels <- FDH_hist_labels
  
  for (i in seq_along(FDH_hist_layers)) {
    r_fd <- FDH_hist_layers[[i]]
    h_i  <- r_fd * alpha
    h_i[r_fd < FDH_crit] <- NA
    
    t_i    <- FDH_hist_times[i]
    dt_vec <- abs(as.numeric(difftime(time_vec, t_i, units = "hours")))
    k_i    <- which.min(dt_vec)
    if (!length(k_i) || !is.finite(k_i)) next
    
    T_i_mat  <- t(T2M_arr[ , , k_i])
    RH_i_mat <- t(RH_arr[ , , k_i] / 100)
    
    r_T_i    <- raster(T_i_mat);  extent(r_T_i)  <- extent(r_template); crs(r_T_i)  <- crs(r_template)
    r_RH_i   <- raster(RH_i_mat); extent(r_RH_i) <- extent(r_template); crs(r_RH_i) <- crs(r_template)
    
    k_start   <- max(1, k_i - (72 - 1))
    idx3      <- k_start:k_i
    Tr3_i_arr <- apply(T2M_arr[ , , idx3, drop = FALSE], c(1, 2), mean, na.rm = TRUE)
    Tr3_i_mat <- t(Tr3_i_arr)
    r_Tr3_i   <- raster(Tr3_i_mat); extent(r_Tr3_i) <- extent(r_template); crs(r_Tr3_i) <- crs(r_template)
    
    score_h_i  <- calc(h_i,    fun = function(h)  score_h_fun(h, h_min, h_opt))
    score_T_i  <- calc(r_T_i,  fun = function(Tv) score_T_fun(Tv, T_opt, T_min, T_max, range_T))
    score_T3_i <- calc(r_Tr3_i,fun = function(T3v)score_T_fun(T3v, T3_opt, T3_min, T3_max, range_T3))
    score_RH_i <- calc(r_RH_i, fun = function(rh) score_RH_fun(rh, RH_opt_c, RH_sig_c))
    
    climb_i <- overlay(score_h_i, score_T_i, score_T3_i, score_RH_i,
                       fun = function(a, b, c, d) a * b * c * d)
    
    climb_i[is.na(h_i) | h_i <= h_min] <- NA
    climb_i[!is.finite(climb_i)]       <- NA
    climb_i[climb_i <= 0]              <- NA
    names(climb_i) <- paste0("Climbability_hist_", climb_hist_labels[i])
    climb_hist_layers[[i]] <- climb_i
  }
}

climb_index_ij <- score_h * score_T * score_T3 * score_RH
climb_index_ij[h_mat_ij <= h_min]          <- NA
climb_index_ij[!is.finite(climb_index_ij)] <- NA
climb_index_ij[climb_index_ij <= 0]        <- NA

climb_r <- raster(t(climb_index_ij))
extent(climb_r) <- extent(r_template)
crs(climb_r)    <- crs(r_template)
names(climb_r)  <- "Climbability_0_1"

# 8) Prognose aus NWP --------------------------------------------------

base_url_nwp   <- "https://dataset.api.hub.geosphere.at/v1/grid/forecast/nwp-v1-1h-2500m"
parameters_nwp <- c("t2m", "rh2m", "u10m", "v10m")
param_str_nwp  <- paste(parameters_nwp, collapse = ",")

t_now    <- max(inca_nordtirol_all$time, na.rm = TRUE)
t0       <- t_now
fc_h_max <- 60

t_fc_end <- t_now + fc_h_max * 3600

start_fc <- format(t_now,    "%Y-%m-%dT%H:%M")
end_fc   <- format(t_fc_end, "%Y-%m-%dT%H:%M")

outfile_fc <- file.path(
  out_dir_nwp,
  sprintf("nwp_nordtirol_%s_%sh.nc",
          format(as.Date(t_now), "%Y%m%d"),
          fc_h_max)
)

if (!file.exists(outfile_fc) || file.info(outfile_fc)$size == 0) {
  message("NWP-Chunk: ", start_fc, " bis ", end_fc)
  
  query_fc <- list(
    parameters    = param_str_nwp,
    start         = start_fc,
    end           = end_fc,
    bbox          = paste(bbox, collapse = ","),
    output_format = "netcdf",
    filename      = "nwp_nordtirol"
  )
  
  resp_fc <- GET(
    url   = base_url_nwp,
    query = query_fc,
    write_disk(outfile_fc, overwrite = TRUE)
  )
  stop_for_status(resp_fc)
  message("  -> NWP-Vorhersage gespeichert: ", outfile_fc,
          " (", file.info(outfile_fc)$size, " Bytes)")
} else {
  message("  -> NWP-Vorhersage übersprungen (existiert): ", outfile_fc)
}

ice_fc_layers   <- NULL
climb_fc_layers <- NULL
fc_step_hours   <- NULL

if (file.exists(outfile_fc)) {
  nc_fc <- nc_open(outfile_fc)
  
  time_dim_name_fc <- names(nc_fc$dim)[grep("time", tolower(names(nc_fc$dim)))[1]]
  time_fc_vals     <- ncvar_get(nc_fc, time_dim_name_fc)
  time_fc_units    <- ncatt_get(nc_fc, time_dim_name_fc, "units")$value
  time_fc          <- convert_nc_time(time_fc_vals, time_fc_units)
  
  nc_close(nc_fc)
  
  lead_hours <- as.numeric(difftime(time_fc, t_now, units = "hours"))
  keep_fc    <- which(lead_hours >= 0 & lead_hours <= fc_h_max + 0.01)
  
  if (length(keep_fc) > 0) {
    time_fc    <- time_fc[keep_fc]
    lead_hours <- lead_hours[keep_fc]
    
    T2M_fc_brick  <- brick(outfile_fc, varname = "t2m")[[keep_fc]]
    RH2M_fc_brick <- brick(outfile_fc, varname = "rh2m")[[keep_fc]]
    U10_fc_brick  <- brick(outfile_fc, varname = "u10m")[[keep_fc]]
    V10_fc_brick  <- brick(outfile_fc, varname = "v10m")[[keep_fc]]
    
    crs(T2M_fc_brick)  <- crs(r_template)
    crs(RH2M_fc_brick) <- crs(r_template)
    crs(U10_fc_brick)  <- crs(r_template)
    crs(V10_fc_brick)  <- crs(r_template)
    
    T2M_fc  <- resample(T2M_fc_brick,  r_template, method = "bilinear")
    RH2M_fc <- resample(RH2M_fc_brick, r_template, method = "bilinear")
    U10_fc  <- resample(U10_fc_brick,  r_template, method = "bilinear")
    V10_fc  <- resample(V10_fc_brick,  r_template, method = "bilinear")
    
    W_fc <- overlay(U10_fc, V10_fc, fun = function(u, v) sqrt(u^2 + v^2))
    
    time_local_fc <- as.POSIXlt(time_fc, tz = "Europe/Vienna")
    doy_fc  <- time_local_fc$yday + 1
    hour_fc <- time_local_fc$hour + time_local_fc$min / 60 + time_local_fc$sec / 3600
    
    delta_fc <- 23.44 * pi/180 * sin(2 * pi * (284 + doy_fc) / 365)
    H_fc     <- (hour_fc - 12) * 15 * pi/180
    
    sin_alpha_fc <- sin(lat_center_rad) * sin(delta_fc) +
      cos(lat_center_rad) * cos(delta_fc) * cos(H_fc)
    sin_alpha_fc[sin_alpha_fc < 0] <- 0
    solar_height_factor_fc <- sin_alpha_fc
    
    weight_time_fc <- rep(1, length(time_fc))
    
    Tr3_fc <- calc(T2M_fc, fun = function(x) mean(x, na.rm = TRUE))
    score_T3_fc <- calc(Tr3_fc, fun = function(t3) {
      score_T_fun(t3, T3_opt, T3_min, T3_max, range_T3)
    })
    
    time_fc_local <- as.POSIXct(format(time_fc, tz = "Europe/Vienna", usetz = TRUE),
                                tz = "Europe/Vienna")
    
    t0_local_fc <- as.POSIXct(format(t0, tz = "Europe/Vienna", usetz = TRUE),
                              tz = "Europe/Vienna")
    
    target_hours_all  <- c(0, lead_hours)
    target_labels_all <- c(
      paste0(format(t0_local_fc, "%d.%m.%Y, %H:%M"), " (Analyse)"),
      format(time_fc_local, "%d.%m.%Y, %H:%M")
    )
    
    ord <- order(target_hours_all)
    target_hours_all  <- target_hours_all[ord]
    target_labels_all <- target_labels_all[ord]
    
    max_lead <- max(lead_hours) + 1e-6
    keep_idx <- target_hours_all <= max_lead
    target_hours_all  <- target_hours_all[keep_idx]
    target_labels_all <- target_labels_all[keep_idx]
    
    target_times <- t0 + target_hours_all * 3600
    
    ice_fc_layers   <- vector("list", length(target_hours_all))
    climb_fc_layers <- vector("list", length(target_hours_all))
    names(ice_fc_layers)   <- target_labels_all
    names(climb_fc_layers) <- target_labels_all
    
    if (length(target_hours_all) > 0 && target_hours_all[1] == 0) {
      ice_fc_layers[[1]]   <- ice_thick_expo
      climb_fc_layers[[1]] <- climb_r
    }
    
    FDH_base_r  <- r_FDH_eff
    delta_cum_r <- raster(FDH_base_r); delta_cum_r[] <- 0
    
    next_target_idx <- which(target_hours_all > 0)[1]
    if (is.na(next_target_idx)) next_target_idx <- length(target_hours_all) + 1
    
    for (k in seq_along(time_fc)) {
      dt_k <- lead_hours[k]
      
      T_k  <- raster(T2M_fc,  layer = k)
      RH_k <- raster(RH2M_fc, layer = k) / 100
      W_k  <- raster(W_fc,    layer = k)
      
      FDH_k <- calc(T_k, fun = function(x) pmax(-x, 0))
      MDH_k <- calc(T_k, fun = function(x) pmax( x, 0))
      
      f_wind <- calc(W_k, fun = function(w) {
        wn <- (w - wind_ref) / wind_ref
        f  <- 1 + k_wind * wn
        f[!is.finite(f)] <- 1
        pmax(wind_min, pmin(wind_max, f))
      })
      
      f_rh <- calc(RH_k, fun = function(rh) {
        0.5 + 0.5 * exp(- (rh - rh_opt)^2 / (2 * rh_sig^2))
      })
      
      s_height_k <- solar_height_factor_fc[k]
      f_rad_k <- if (s_height_k == 0) {
        solar_index_r * 0 + 1
      } else {
        calc(solar_index_r, fun = function(s_index) {
          f <- 1 - k_expo * s_height_k * s_index
          f[!is.finite(f)] <- 1
          pmax(0, pmin(1, f))
        })
      }
      
      f_time_k <- weight_time_fc[k]
      
      FDH_eff_k <- FDH_k * f_wind * f_rh * f_rad_k * f_time_k
      MDH_eff_k <- MDH_k * k_melt * f_time_k
      
      delta_cum_r <- delta_cum_r + (FDH_eff_k - MDH_eff_k)
      
      while (next_target_idx <= length(target_hours_all) &&
             dt_k >= target_hours_all[next_target_idx] - 0.5 &&
             is.null(ice_fc_layers[[next_target_idx]])) {
        
        FDH_fc_k <- FDH_base_r + delta_cum_r
        FDH_fc_k[FDH_fc_k < 0] <- 0
        
        ice_k <- FDH_fc_k * alpha
        ice_k[FDH_fc_k < FDH_crit] <- NA
        names(ice_k) <- paste0("h_pot_expo_m_fc_", target_hours_all[next_target_idx], "h")
        ice_fc_layers[[next_target_idx]] <- ice_k
        
        h_k <- ice_k
        
        score_h_k  <- calc(h_k, fun = function(h)  score_h_fun(h, h_min, h_opt))
        score_T_k  <- calc(T_k, fun = function(Tv) score_T_fun(Tv, T_opt, T_min, T_max, range_T))
        score_RH_k <- calc(RH_k,fun = function(rh) score_RH_fun(rh, RH_opt_c, RH_sig_c))
        
        climb_k <- overlay(score_h_k, score_T_k, score_T3_fc, score_RH_k,
                           fun = function(a, b, c, d) a * b * c * d)
        
        climb_k[is.na(h_k) | h_k <= h_min] <- NA
        climb_k[!is.finite(climb_k)]       <- NA
        climb_k[climb_k <= 0]              <- NA
        names(climb_k) <- paste0("Climbability_fc_", target_hours_all[next_target_idx], "h")
        climb_fc_layers[[next_target_idx]] <- climb_k
        
        next_target_idx <- next_target_idx + 1
      }
    }
    
    if (any(vapply(ice_fc_layers, is.null, logical(1)))) {
      last_non_null <- max(which(!vapply(ice_fc_layers, is.null, logical(1))))
      for (i in seq_along(ice_fc_layers)) {
        if (is.null(ice_fc_layers[[i]])) {
          ice_fc_layers[[i]]   <- ice_fc_layers[[last_non_null]]
          climb_fc_layers[[i]] <- climb_fc_layers[[last_non_null]]
        }
      }
    }
    
    fc_step_hours <- target_hours_all
  }
}

# 9) Controls / HTML Summary (wie bei dir) -----------------------------
# NOTE: Die "Kletterbarkeit – Tagesübersicht"-Box wurde auf Wunsch entfernt.
#       Der Code zur Berechnung bleibt hier ggf. stehen, wird aber NICHT mehr als Control eingebunden.

best_html <- NULL

# 10) Zeit-Layer zusammenbauen (wie bei dir; Steps bleiben identisch) ---

ice_time_layers   <- list()
climb_time_layers <- list()
time_labels       <- character(0)

if (length(ice_hist_layers) > 0L) {
  ice_time_layers   <- c(ice_time_layers, ice_hist_layers)
  climb_time_layers <- c(climb_time_layers, climb_hist_layers)
  time_labels       <- c(time_labels, ice_hist_labels)
}

if (!is.null(ice_fc_layers) && length(ice_fc_layers) > 0L) {
  ice_time_layers   <- c(ice_time_layers, ice_fc_layers)
  climb_time_layers <- c(climb_time_layers, climb_fc_layers)
  time_labels       <- c(time_labels, names(ice_fc_layers))
}

if (length(ice_time_layers) == 0L) {
  ice_time_layers   <- list(ice_thick_expo)
  climb_time_layers <- list(climb_r)
  time_labels       <- format(Sys.Date(), "%d.%m.%Y (Jetzt)")
}

n_steps <- min(length(ice_time_layers), length(climb_time_layers), length(time_labels))
ice_time_layers   <- ice_time_layers[seq_len(n_steps)]
climb_time_layers <- climb_time_layers[seq_len(n_steps)]
time_labels       <- time_labels[seq_len(n_steps)]

max_h_all <- vapply(
  ice_time_layers,
  function(r) {
    if (is.null(r)) return(NA_real_)
    suppressWarnings(cellStats(r, "max", na.rm = TRUE))
  },
  numeric(1)
)
max_h <- max(max_h_all, na.rm = TRUE)
if (!is.finite(max_h) || max_h <= 0) max_h <- 1

col_fun <- colorRampPalette(c("#ffffff", "#c6dbef", "#6baed6", "#08519c"))

pal_h <- colorNumeric(
  palette  = col_fun(100),
  domain   = c(0, max_h),
  na.color = "transparent"
)

pal_ci <- colorNumeric(
  palette  = rev(terrain.colors(100)),
  domain   = c(0, 1),
  na.color = "transparent"
)

last_update <- format(Sys.time(), tz = "Europe/Vienna", "%d.%m.%Y %H:%M %Z")
last_update_iso <- format(Sys.time(), tz = "UTC", "%Y-%m-%dT%H:%M:%SZ")
last_update_payload <- jsonlite::toJSON(
  list(
    last_update = last_update,
    last_update_iso = last_update_iso
  ),
  auto_unbox = TRUE,
  pretty = TRUE
)
dir.create("site", showWarnings = FALSE)
writeLines(last_update_payload, "site/last_update.json", useBytes = TRUE)
writeLines(last_update_payload, "last_update.json", useBytes = TRUE)
ext         <- extent(r_template)

# =====================================================================
# SPEED PATCH CORE: externe PNGs pro Step schreiben
# =====================================================================

dir.create("site/img", recursive = TRUE, showWarnings = FALSE)
dir.create("site/plots", recursive = TRUE, showWarnings = FALSE)

hex_to_rgba <- function(hex) {
  hex <- gsub("#", "", hex)
  hex[hex == "transparent" | is.na(hex) | nchar(hex) == 0] <- "00000000"
  hex[nchar(hex) == 6] <- paste0(hex[nchar(hex) == 6], "FF")
  r <- strtoi(substr(hex, 1, 2), 16L) / 255
  g <- strtoi(substr(hex, 3, 4), 16L) / 255
  b <- strtoi(substr(hex, 5, 6), 16L) / 255
  a <- strtoi(substr(hex, 7, 8), 16L) / 255
  cbind(r, g, b, a)
}

write_overlay_png <- function(r, pal, file) {
  v <- raster::getValues(r)
  cols <- pal(v)
  cols[!is.finite(v)] <- "#00000000"
  cols[cols == "transparent"] <- "#00000000"
  
  rgba <- hex_to_rgba(cols)
  nr <- raster::nrow(r)
  nc <- raster::ncol(r)
  
  arr <- array(0, dim = c(nr, nc, 4))
  arr[, , 1] <- matrix(rgba[,1], nr, nc, byrow = TRUE)
  arr[, , 2] <- matrix(rgba[,2], nr, nc, byrow = TRUE)
  arr[, , 3] <- matrix(rgba[,3], nr, nc, byrow = TRUE)
  arr[, , 4] <- matrix(rgba[,4], nr, nc, byrow = TRUE)
  
  png::writePNG(arr, target = file)
}

message("Schreibe PNG-Overlays für ", n_steps, " Steps …")
for (i in seq_len(n_steps)) {
  write_overlay_png(ice_time_layers[[i]],   pal_h,  sprintf("site/img/ice_%03d.png", i))
  write_overlay_png(climb_time_layers[[i]], pal_ci, sprintf("site/img/climb_%03d.png", i))
}

# 11) Eisfall-Sonnendaten + Topo-URLs ---------------------------------

DIR_SUN <- "data/suntime"

find_sun_file_for_uid <- function(uid, dir_sun = DIR_SUN) {
  uid_i <- suppressWarnings(as.integer(uid))
  if (!is.finite(uid_i)) return(NA_character_)
  cand <- c(
    file.path(dir_sun, sprintf("sun_uid_%03d.csv", uid_i)),
    file.path(dir_sun, sprintf("sun_uid_%d.csv", uid_i))
  )
  hit <- cand[file.exists(cand)]
  if (length(hit) == 0) return(NA_character_)
  hit[[1]]
}

load_sun_for_uids <- function(uids, dir_sun = DIR_SUN) {
  out <- vector("list", length(uids))
  missing_uids <- integer(0)
  for (i in seq_along(uids)) {
    uid <- as.integer(uids[[i]])
    f <- find_sun_file_for_uid(uid, dir_sun = dir_sun)
    if (is.na(f)) {
      missing_uids <- c(missing_uids, uid)
      next
    }
    df <- tryCatch(readr::read_csv(f, show_col_types = FALSE), error = function(e) NULL)
    if (is.null(df) || nrow(df) == 0) {
      missing_uids <- c(missing_uids, uid)
      next
    }
    out[[i]] <- df
  }
  list(
    data = dplyr::bind_rows(out),
    missing_uids = sort(unique(missing_uids))
  )
}

# Optional meta (difficulty) for map filters
PATH_META <- "data/Koordinaten_Wasserfaelle/eisklettern_links_entries_diff.csv"
meta_map <- NULL
if (file.exists(PATH_META)) {
  parse_uid <- function(x) {
    as.integer(readr::parse_number(as.character(x)))
  }
  get_chr <- function(df, ...) {
    cands <- c(...)
    for (nm in cands) if (nm %in% names(df)) return(as.character(df[[nm]]))
    rep(NA_character_, nrow(df))
  }
  to_num <- function(x) {
    if (is.null(x)) return(NA_real_)
    x <- as.character(x)
    x[x %in% c("", "NA", "NaN", "NULL")] <- NA_character_
    x <- gsub(",", ".", x, fixed = TRUE)
    suppressWarnings(as.numeric(x))
  }
  meta_raw <- readr::read_delim(
    PATH_META,
    delim = ";",
    col_types = readr::cols(.default = readr::col_character()),
    show_col_types = FALSE
  ) %>%
    rename_with(tolower)
  meta_map <- tibble(
    uid = parse_uid(meta_raw$uid),
    name = get_chr(meta_raw, "name", "eisfall", "icefall"),
    difficulty = get_chr(meta_raw, "schwierigkeit", "difficulty", "grad"),
    topo_url = get_chr(meta_raw, "topo_url"),
    latitude = to_num(get_chr(meta_raw, "latitude")),
    longitude = to_num(get_chr(meta_raw, "longitude"))
  )
}

if (!dir.exists(DIR_SUN)) {
  message("⚠️ Sun-Verzeichnis fehlt: ", DIR_SUN)
}

expected_uids <- if (!is.null(meta_map) && "uid" %in% names(meta_map)) {
  sort(unique(meta_map$uid[is.finite(meta_map$uid)]))
} else {
  integer(0)
}

if (length(expected_uids) > 0) {
  if (dir.exists(DIR_SUN)) {
    sun_loaded <- load_sun_for_uids(expected_uids, dir_sun = DIR_SUN)
    if (length(sun_loaded$missing_uids) > 0) {
      message(
        "⚠️ Fehlende Sun-Dateien für UIDs: ",
        paste(sprintf("%03d", sun_loaded$missing_uids), collapse = ", ")
      )
    }
    sun_df <- sun_loaded$data
  } else {
    message(
      "⚠️ Fehlende Sun-Dateien für UIDs: ",
      paste(sprintf("%03d", expected_uids), collapse = ", ")
    )
    sun_df <- tibble::tibble()
  }
} else {
  if (dir.exists(DIR_SUN)) {
    sun_files <- sort(list.files(DIR_SUN, pattern = "^sun_uid_\\d+\\.csv$", full.names = TRUE))
    if (length(sun_files) == 0) {
      message("⚠️ Keine Sun-Dateien gefunden in: ", DIR_SUN)
      sun_df <- tibble::tibble()
    } else {
      sun_df <- dplyr::bind_rows(lapply(sun_files, function(f) readr::read_csv(f, show_col_types = FALSE)))
    }
  } else {
    sun_df <- tibble::tibble()
  }
}

sun_missing_uids <- if (length(expected_uids) > 0) {
  setdiff(expected_uids, unique(sun_df$uid[is.finite(sun_df$uid)]))
} else {
  integer(0)
}

if (nrow(sun_df) == 0) {
  sun_df <- tibble::tibble(
    uid = integer(),
    date = as.Date(character()),
    sunrise_topo = as.POSIXct(character(), tz = "UTC"),
    sunset_topo = as.POSIXct(character(), tz = "UTC"),
    sun_hours_topo = numeric()
  )
}

sun_df <- sun_df %>%
  dplyr::mutate(
    uid  = as.integer(readr::parse_number(as.character(uid))),
    date = as.Date(date)
  )

if (!inherits(sun_df$sunrise_topo, "POSIXt")) {
  sun_df <- sun_df %>%
    mutate(
      sunrise_topo = ymd_hms(sunrise_topo, tz = "UTC"),
      sunset_topo  = ymd_hms(sunset_topo,  tz = "UTC")
    )
}

sun_df <- sun_df %>%
  mutate(
    sunrise_topo = with_tz(sunrise_topo, "Europe/Vienna"),
    sunset_topo  = with_tz(sunset_topo,  "Europe/Vienna"),
    sun_hours_topo = as.numeric(difftime(sunset_topo, sunrise_topo, units = "hours"))
  )

sun_date <- as.Date(Sys.time(), tz = "Europe/Vienna")

sun_today <- sun_df %>% dplyr::filter(date == sun_date)

if (nrow(sun_today) == 0) {
  last_date <- max(sun_df$date, na.rm = TRUE)
  message("Keine Sonnendaten für ", sun_date, " gefunden. Verwende stattdessen ", last_date, ".")
  sun_today <- sun_df %>% dplyr::filter(date == last_date)
}

if (nrow(sun_today) == 0) {
  message("⚠️ sun_today ist leer – es werden keine Sonnenzeiten angezeigt.")
}

if (!is.null(meta_map)) {
  marker_base <- meta_map %>%
    dplyr::filter(is.finite(uid)) %>%
    dplyr::select(uid, name, latitude, longitude, topo_url, difficulty) %>%
    dplyr::arrange(uid) %>%
    dplyr::distinct(uid, .keep_all = TRUE)

  if (nrow(marker_base) == 0) {
    message("⚠️ Meta-Daten enthalten keine gültigen UIDs – verwende Sun-Daten als Markerbasis.")
    sun_today <- sun_today
  } else {
    sun_today <- marker_base %>%
      dplyr::left_join(
        sun_today %>%
          dplyr::select(uid, name, date, sunrise_topo, sunset_topo, sun_hours_topo),
        by = "uid",
        suffix = c("_meta", "_sun")
      ) %>%
      dplyr::mutate(
        name = dplyr::coalesce(.data$name_meta, .data$name_sun),
        date = dplyr::coalesce(.data$date, sun_date),
        sun_status = dplyr::case_when(
          uid %in% sun_missing_uids ~ "missing_file",
          is.finite(sun_hours_topo) & !is.na(sunrise_topo) & !is.na(sunset_topo) ~ "has_values",
          TRUE ~ "no_values_day"
        )
      ) %>%
      dplyr::select(-dplyr::any_of(c("name_meta", "name_sun")))
  }
} else {
  sun_today <- sun_today %>%
    dplyr::mutate(
      difficulty = NA_character_,
      sun_status = dplyr::if_else(
        is.finite(sun_hours_topo) & !is.na(sunrise_topo) & !is.na(sunset_topo),
        "has_values",
        "no_values_day"
      )
    )
}

if (!"topo_url" %in% names(sun_today)) {
  sun_today$topo_url <- NA_character_
}
if (!"latitude" %in% names(sun_today)) {
  sun_today$latitude <- NA_real_
}
if (!"longitude" %in% names(sun_today)) {
  sun_today$longitude <- NA_real_
}

sun_today <- sun_today %>%
  dplyr::mutate(
    name = dplyr::coalesce(name, paste0("Eisfall ", sprintf("%03d", uid))),
    sunrise_txt   = substr(as.character(sunrise_topo), 12, 16),
    sunset_txt    = substr(as.character(sunset_topo),  12, 16),
    sun_hours_txt = sprintf("%.1f", sun_hours_topo),
    date_txt      = format(date, "%d.%m.%Y"),
    link_txt = ifelse(
      !is.na(topo_url) & topo_url != "",
      paste0("<a href='", topo_url, "' target='_blank'>Topo öffnen</a>"),
      "(kein Topo-Link hinterlegt)"
    ),
    
    uid_pad  = sprintf("%03d", uid),
    plot_png = paste0("plots/uid_", uid_pad, ".png"),
    
    detail_url = paste0("icefalls/uid_", uid_pad, ".html"),
    
    map_meta = paste0(
      "<span class='map-meta' data-uid='", uid,
      "' data-name='", htmltools::htmlEscape(ifelse(is.na(name), "", name), attribute = TRUE),
      "' data-difficulty='", htmltools::htmlEscape(ifelse(is.na(difficulty), "", difficulty), attribute = TRUE),
      "' data-sun='", ifelse(is.na(sun_hours_topo), "", sprintf("%.2f", sun_hours_topo)),
      "'></span>"
    ),
    
    plot_block = paste0(
      "<hr style='margin:6px 0;'/>",
      
      "<div style='display:flex; gap:6px; flex-wrap:wrap;'>",
      
      "<a href='", detail_url, "' ",
      "style='padding:6px 10px; background:#0d6efd; color:white; ",
      "border-radius:6px; text-decoration:none; font-weight:600;'>",
      "📄 Details & Upload",
      "</a>",

      "</div>",
      
      "<a href='", plot_png, "' target='_blank'>",
      "<img src='", plot_png, "' ",
      "style='width:320px;max-width:100%;height:auto;",
      "border:1px solid #ccc;border-radius:4px;margin-top:6px;' ",
      "onerror=\"this.style.display='none';\"/>",
      "</a>"
    ),
    
    popup = ifelse(
      sun_status == "missing_file",
      paste0(
        map_meta,
        sprintf(
          "<b>%s</b><br/>Sonne am %s: KEINE SONNENDATEN<br/>%s",
          name, date_txt, link_txt
        ),
        plot_block
      ),
      ifelse(
        is.na(sun_hours_topo) | is.na(sunrise_topo) | is.na(sunset_topo),
        paste0(
          map_meta,
          sprintf(
          "<b>%s</b><br/>Sonne am %s: keine direkte Sonneneinstrahlung<br/>%s",
          name, date_txt, link_txt
          ),
          plot_block
        ),
        paste0(
          map_meta,
          sprintf(
            "<b>%s</b><br/>Sonne am %s: %s – %s (%s h)<br/>%s",
            name, date_txt, sunrise_txt, sunset_txt, sun_hours_txt, link_txt
          ),
          plot_block
        )
      )
    )
  )

marker_data <- sun_today %>%
  dplyr::filter(is.finite(latitude), is.finite(longitude))

if (nrow(marker_data) == 0) {
  message("⚠️ Keine Marker mit gültigen Koordinaten verfügbar – Karte zeigt keine Eisfälle.")
} else {
  message("✅ Marker bereit: ", nrow(marker_data), " Eisfälle mit gültigen Koordinaten.")
}

# 12) Leaflet Map: nur 2 Overlays + Slider wechselt URL ----------------

init_i <- n_steps
m <- leaflet() |>
  addProviderTiles(providers$OpenStreetMap, group = "OSM") |>
  addProviderTiles(providers$OpenTopoMap,   group = "Gelände (Topo)")

# ✅ (Entfernt) Kletterbarkeit – Tagesübersicht (Prognose)
# if (!is.null(best_html)) {
#   m <- m |>
#     addControl(position = "bottomright", html = best_html)
# }

preview_mode <- interactive()

if (isTRUE(preview_mode)) {
  m <- m |>
    addRasterImage(
      ice_time_layers[[init_i]],
      colors  = pal_h,
      opacity = 0.8,
      project = TRUE,
      method  = 'bilinear',
      group   = 'Eisdicke',
      layerId = 'ice_preview'
    ) |>
    addRasterImage(
      climb_time_layers[[init_i]],
      colors  = pal_ci,
      opacity = 0.7,
      project = TRUE,
      method  = 'bilinear',
      group   = 'Climbability',
      layerId = 'climb_preview'
    )
  
} else {
  bounds_js <- sprintf("[[%f,%f],[%f,%f]]", ext@ymin, ext@xmin, ext@ymax, ext@xmax)
  
  m <- htmlwidgets::onRender(
    m,
    sprintf(
      "function(el, x) {
         var map = this;
         var bounds = %s;
         function pad3(n){ return String(n).padStart(3,'0'); }

         var iceUrl   = 'img/ice_'   + pad3(%d) + '.png';
         var climbUrl = 'img/climb_' + pad3(%d) + '.png';

         var rasterPane = map.getPane('iceRasterPane');
         if (!rasterPane && typeof map.createPane === 'function') {
           rasterPane = map.createPane('iceRasterPane');
         }
         if (rasterPane && rasterPane.style) {
           rasterPane.style.zIndex = 250;
         }

         var ice   = L.imageOverlay(iceUrl,   bounds, {opacity: 0.8, layerId: 'ice_overlay', pane: 'iceRasterPane'});
         var climb = L.imageOverlay(climbUrl, bounds, {opacity: 0.7, layerId: 'climb_overlay', pane: 'iceRasterPane'});

         try {
           if (map.layerManager && typeof map.layerManager.addLayer === 'function') {
             map.layerManager.addLayer(ice,   'image', 'ice_overlay',   'Eisdicke',     null, null);
             map.layerManager.addLayer(climb, 'image', 'climb_overlay', 'Climbability', null, null);

             if (typeof map.layerManager.showGroup === 'function') {
               map.layerManager.showGroup('Eisdicke');
               map.layerManager.showGroup('Climbability');
             }
           } else {
             ice.addTo(map);
             climb.addTo(map);
           }
         } catch(e) {
           try { ice.addTo(map); climb.addTo(map); } catch(e2) {}
         }

         window._iceOverlay   = ice;
         window._climbOverlay = climb;
       }",
      bounds_js,
      init_i,
      init_i
    )
  )
}

m <- m |>
  addMapPane("icefallsPane", zIndex = 650) |>
  addControl(
    position = "topleft",
    html = htmltools::HTML(
      "<div style='background:rgba(255,255,255,0.9);padding:6px 8px;border-radius:6px;display:flex;flex-direction:column;gap:6px;'>
         <a href='index.html' class='map-home-link' style='font-size:14px;font-weight:bold;'>🏠 Startseite</a>
         <a href='list.html' class='map-list-link' style='font-size:14px;font-weight:bold;'>📋 Eisfall-Liste</a>
       </div>"
    )
  )  |>
  addControl(
    position = "bottomright",
    html = htmltools::HTML(
      "<details id='map-filter' style='background:rgba(255,255,255,0.95);padding:8px 10px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.12);min-width:240px;width:340px;max-width:calc(100vw - 24px);box-sizing:border-box;'>\n        <summary style='font-weight:700;font-size:12px;letter-spacing:0.02em;text-transform:uppercase;cursor:pointer;'>Filter</summary>\n        <div style='margin-top:6px;display:flex;flex-direction:column;gap:10px;'>\n          <input id='mapFilterInput' type='search' placeholder='Name, UID, Schwierigkeit' autocomplete='off' style='width:100%;padding:6px 8px;border:1px solid #ddd;border-radius:8px;font-size:13px;'/>\n          <div style='display:flex;flex-direction:column;gap:6px;'>\n            <div style='font-size:11px;color:#444;font-weight:700;letter-spacing:0.02em;text-transform:uppercase;'>Schwierigkeit</div>\n            <div style='display:flex;flex-direction:column;gap:6px;'>\n              <div style='display:flex;flex-direction:column;gap:4px;font-size:11px;color:#555;'><span>Technisch (A)</span><div style='display:flex;gap:6px;align-items:center;flex-wrap:wrap;'><input id='mapAmin' type='range' min='0.75' max='4.25' step='0.25' value='0.75' style='flex:1;min-width:120px;'/><input id='mapAmax' type='range' min='0.75' max='4.25' step='0.25' value='4.25' style='flex:1;min-width:120px;'/><span id='mapARangeTxt' style='font-size:11px;color:#666;min-width:90px;'>A1- – A4+</span></div></div>\n              <div style='display:flex;flex-direction:column;gap:4px;font-size:11px;color:#555;'><span>Mixed (M)</span><div style='display:flex;gap:6px;align-items:center;flex-wrap:wrap;'><input id='mapMmin' type='range' min='0.75' max='13.25' step='0.25' value='0.75' style='flex:1;min-width:120px;'/><input id='mapMmax' type='range' min='0.75' max='13.25' step='0.25' value='13.25' style='flex:1;min-width:120px;'/><span id='mapMRangeTxt' style='font-size:11px;color:#666;min-width:90px;'>M1- – M13+</span></div></div>\n              <div style='display:flex;flex-direction:column;gap:4px;font-size:11px;color:#555;'><span>Wassereis (WI)</span><div style='display:flex;gap:6px;align-items:center;flex-wrap:wrap;'><input id='mapWImin' type='range' min='0.75' max='7.25' step='0.25' value='0.75' style='flex:1;min-width:120px;'/><input id='mapWImax' type='range' min='0.75' max='7.25' step='0.25' value='7.25' style='flex:1;min-width:120px;'/><span id='mapWIRangeTxt' style='font-size:11px;color:#666;min-width:90px;'>WI1- – WI7+</span></div></div>\n              <div style='display:flex;flex-direction:column;gap:4px;font-size:11px;color:#555;'><span>Fels (UIAA)</span><div style='display:flex;gap:6px;align-items:center;flex-wrap:wrap;'><input id='mapRmin' type='range' min='0.75' max='12.25' step='0.25' value='0.75' style='flex:1;min-width:120px;'/><input id='mapRmax' type='range' min='0.75' max='12.25' step='0.25' value='12.25' style='flex:1;min-width:120px;'/><span id='mapRRangeTxt' style='font-size:11px;color:#666;min-width:90px;'>1- – 12+</span></div></div>\n            </div>\n          </div>\n          <div style='display:flex;flex-direction:column;gap:6px;'>\n            <div style='font-size:11px;color:#444;font-weight:700;letter-spacing:0.02em;text-transform:uppercase;'>Sonne</div>\n            <div style='display:flex;flex-direction:column;gap:4px;font-size:11px;color:#555;'><span>Sonne morgen (h)</span><div style='display:flex;gap:6px;align-items:center;flex-wrap:wrap;'><input id='mapSunMin' type='range' min='0' max='12' step='0.25' value='0' style='flex:1;min-width:120px;'/><input id='mapSunMax' type='range' min='0' max='12' step='0.25' value='12' style='flex:1;min-width:120px;'/><span id='mapSunRangeTxt' style='font-size:11px;color:#666;min-width:90px;'>0.0 – 12.0 h</span></div></div>\n          </div>\n          <button id='mapFilterReset' type='button' style='border:1px solid #d1d5db;background:#fff;border-radius:8px;padding:6px 8px;font-size:12px;cursor:pointer;'>Filter zurücksetzen</button>\n          <div id='mapFilterStatus' style='font-size:12px;color:#666;line-height:1.2;min-height:1.2em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;width:100%;'></div>\n        </div>\n      </details>"
    )
  )  |>
  addControl(
    position = "bottomright",
    html = htmltools::HTML(
      "<button id='mapUseGeo' type='button' title='GPS' style='width:34px;height:34px;border-radius:999px;border:1px solid #d1d5db;background:#fff;box-shadow:0 4px 10px rgba(0,0,0,0.12);font-size:16px;cursor:pointer;margin-top:8px;'>📍</button>"
    )
  ) |>
  htmlwidgets::onRender(
    "function(el, x) {
       var style = document.createElement('style');
       style.textContent = \"@media (max-width: 720px){\" +
         \".leaflet-control .map-list-link,.leaflet-control .map-home-link{display:inline-flex;justify-content:center;width:100%;font-size:13px;padding:8px 10px;}\" +
         \"#map-filter{min-width:180px;}\" +
         \".leaflet-control{max-width:calc(100vw - 24px);}\" +
         \"}\" +
         \"#map-filter summary{list-style:none;}\" +
         \"#map-filter summary::-webkit-details-marker{display:none;}\";
       el.appendChild(style);
     }"
  ) |>
  htmlwidgets::onRender(
    "function(el, x) {
       var map = this;
       var input = el.querySelector('#mapFilterInput');
       var status = el.querySelector('#mapFilterStatus');
       var diffInput = el.querySelector('#mapDifficultyInput');
       var geoBtn = el.querySelector('#mapUseGeo');
       var geoStatus = el.querySelector('#mapGeoStatus');
       var aMin = el.querySelector('#mapAmin');
       var aMax = el.querySelector('#mapAmax');
       var mMin = el.querySelector('#mapMmin');
       var mMax = el.querySelector('#mapMmax');
       var wiMin = el.querySelector('#mapWImin');
       var wiMax = el.querySelector('#mapWImax');
       var rMin = el.querySelector('#mapRmin');
       var rMax = el.querySelector('#mapRmax');
       var sunMin = el.querySelector('#mapSunMin');
       var sunMax = el.querySelector('#mapSunMax');
       var aRangeTxt = el.querySelector('#mapARangeTxt');
       var mRangeTxt = el.querySelector('#mapMRangeTxt');
       var wiRangeTxt = el.querySelector('#mapWIRangeTxt');
       var rRangeTxt = el.querySelector('#mapRRangeTxt');
       var sunRangeTxt = el.querySelector('#mapSunRangeTxt');
       var resetBtn = el.querySelector('#mapFilterReset');
       if (!input || !status) return;
       if (typeof L !== 'undefined') {
         var filterBox = el.querySelector('#map-filter');
         if (filterBox) {
           L.DomEvent.disableClickPropagation(filterBox);
           L.DomEvent.disableScrollPropagation(filterBox);
         }
         if (geoBtn) {
           L.DomEvent.disableClickPropagation(geoBtn);
           L.DomEvent.disableScrollPropagation(geoBtn);
         }
       }

       function getGroup() {
         if (map.layerManager) {
           if (typeof map.layerManager.getLayerGroup === 'function') {
             return map.layerManager.getLayerGroup('Eisfälle');
           }
           if (map.layerManager._byGroup && map.layerManager._byGroup['Eisfälle']) {
             return map.layerManager._byGroup['Eisfälle'];
           }
         }
         return null;
       }

       var group = getGroup();
       if (!group || typeof group.getLayers !== 'function' || typeof group.addLayer !== 'function' || typeof group.clearLayers !== 'function') {
         status.textContent = 'Filter derzeit nicht verfügbar.';
         return;
       }

       function ensureGroupVisible(groupName, layerGroup){
         try {
           if (map.layerManager && typeof map.layerManager.showGroup === 'function' && groupName) {
             map.layerManager.showGroup(groupName);
           }
         } catch(e) {}
         try {
           if (layerGroup && typeof layerGroup.addTo === 'function' && typeof map.hasLayer === 'function' && !map.hasLayer(layerGroup)) {
             layerGroup.addTo(map);
           }
         } catch(e) {}
       }

       ensureGroupVisible('Eisfälle', group);

       function num(x){
         var n = Number(x);
         return isFinite(n) ? n : NaN;
       }

       function fmtGrade(v){
         var x = num(v);
         if (!isFinite(x)) return '';
         var base = Math.round(x);
         var diff = x - base;
         if (diff > 0.10) return String(base) + '+';
         if (diff < -0.10) return String(base) + '-';
         return String(base);
       }

       function clampMinMax(minEl, maxEl){
         if (!minEl || !maxEl) return [NaN, NaN];
         var a = Number(minEl.value);
         var b = Number(maxEl.value);
         if (!isFinite(a) || !isFinite(b)) return [NaN, NaN];
         if (a > b) { var t = a; a = b; b = t; minEl.value = a; maxEl.value = b; }
         return [a, b];
       }

       function isDefaultRange(minEl, maxEl){
         if (!minEl || !maxEl) return true;
         var minVal = Number(minEl.value);
         var maxVal = Number(maxEl.value);
         var minDefault = Number(minEl.min);
         var maxDefault = Number(maxEl.max);
         if (!isFinite(minVal) || !isFinite(maxVal)) return true;
         return Math.abs(minVal - minDefault) < 1e-6 && Math.abs(maxVal - maxDefault) < 1e-6;
       }

       function updateRangeLabels(){
         var a = clampMinMax(aMin, aMax);
         var m = clampMinMax(mMin, mMax);
         var w = clampMinMax(wiMin, wiMax);
         var r = clampMinMax(rMin, rMax);
         var s = clampMinMax(sunMin, sunMax);
         if (aRangeTxt && isFinite(a[0]) && isFinite(a[1])) aRangeTxt.textContent = 'A' + fmtGrade(a[0]) + ' – A' + fmtGrade(a[1]);
         if (mRangeTxt && isFinite(m[0]) && isFinite(m[1])) mRangeTxt.textContent = 'M' + fmtGrade(m[0]) + ' – M' + fmtGrade(m[1]);
         if (wiRangeTxt && isFinite(w[0]) && isFinite(w[1])) wiRangeTxt.textContent = 'WI' + fmtGrade(w[0]) + ' – WI' + fmtGrade(w[1]);
         if (rRangeTxt && isFinite(r[0]) && isFinite(r[1])) rRangeTxt.textContent = fmtGrade(r[0]) + ' – ' + fmtGrade(r[1]);
         if (sunRangeTxt && isFinite(s[0]) && isFinite(s[1])) sunRangeTxt.textContent = s[0].toFixed(1) + ' – ' + s[1].toFixed(1) + ' h';
       }

       function formatRange(label, text, fallback){
         var out = text || fallback || '';
         if (!out) return label;
         if (out.indexOf(label) === 0) return out;
         return label + ' ' + out;
       }

       function parseDifficulty(d){
         var s = String(d || '').toUpperCase();
         s = s.replace(/SCHWIERIGKEIT|DIFFICULTY|GRADE|GRAD/g, ' ');
         var out = { a: NaN, m: NaN, wi: NaN, r: NaN };
         function signed(base, sign){
           var n = Number(base);
           if (!isFinite(n)) return NaN;
           if (sign === '+') return n + 0.25;
           if (sign === '-') return n - 0.25;
           return n;
         }
         var m = s.match(/(?:^|[^A-Z])A\\s*(\\d{1,2})\\s*([+\\-])?/);
         if (m) out.a = signed(m[1], m[2]);
         m = s.match(/(?:^|[^A-Z])M\\s*(\\d{1,2})\\s*([+\\-])?/);
         if (m) out.m = signed(m[1], m[2]);
         m = s.match(/(?:^|[^A-Z])WI\\s*(\\d{1,2})\\s*([+\\-])?/);
         if (m) out.wi = signed(m[1], m[2]);
         var re = /(?:^|[^A-Z0-9])(1[0-2]|[1-9])\\s*([+\\-])?(?=\\b|[^0-9])/g;
         var best = NaN;
         while ((m = re.exec(s)) !== null) {
           var v = signed(m[1], m[2]);
           if (isFinite(v)) best = isFinite(best) ? Math.max(best, v) : v;
         }
         out.r = best;
         return out;
       }

       function layerMeta(layer){
         if (layer && layer._mapMeta) return layer._mapMeta;
         var meta = { name: '', uid: '', difficulty: '', sun: NaN, grades: {a:NaN,m:NaN,wi:NaN,r:NaN} };
         var popup = layer && layer.getPopup ? layer.getPopup() : null;
         var content = popup && popup.getContent ? popup.getContent() : '';
         if (content) {
           var wrapper = document.createElement('div');
           wrapper.innerHTML = content;
           var node = wrapper.querySelector('.map-meta');
           if (node && node.dataset) {
             meta.name = node.dataset.name || '';
             meta.uid = node.dataset.uid || '';
             meta.difficulty = node.dataset.difficulty || '';
             meta.sun = num(node.dataset.sun);
           }
         }
         meta.grades = parseDifficulty(meta.difficulty);
         if (layer) layer._mapMeta = meta;
         return meta;
       }

       function collectMarkers(){
         var out = [];
         function visit(layer){
           if (!layer) return;
           if (typeof layer.getLatLng === 'function') {
             out.push(layer);
             return;
           }
           if (typeof layer.getLayers === 'function') {
             var children = layer.getLayers() || [];
             children.forEach(visit);
           }
         }
         var roots = group.getLayers ? group.getLayers() : [];
         (roots || []).forEach(visit);
         return out;
       }

       var allMarkers = collectMarkers();

      function inRange(v, min, max, minEl, maxEl){
        if (!isFinite(min) && !isFinite(max)) return true;
        if (!isFinite(v)) return isDefaultRange(minEl, maxEl);
        if (isFinite(min) && v < min) return false;
        if (isFinite(max) && v > max) return false;
        return true;
      }

       function resetFilters(){
         input.value = '';
         if (diffInput) diffInput.value = '';
         [[aMin, aMax], [mMin, mMax], [wiMin, wiMax], [rMin, rMax], [sunMin, sunMax]].forEach(function(pair){
           var mn = pair[0], mx = pair[1];
           if (mn && mn.min !== undefined) mn.value = mn.min;
           if (mx && mx.max !== undefined) mx.value = mx.max;
         });
       }

       function applyFilter() {
         var term = input.value.trim().toLowerCase();
         var diffTerm = diffInput ? diffInput.value.trim().toLowerCase() : '';
         if (!allMarkers.length) allMarkers = collectMarkers();
         if (!allMarkers.length) {
           status.textContent = 'Filter wird geladen...';
           if (!map._filterRetry) {
             map._filterRetry = true;
             setTimeout(function(){
               map._filterRetry = false;
               applyFilter();
             }, 300);
           }
           return;
         }

        updateRangeLabels();
         var a = clampMinMax(aMin, aMax);
         var m = clampMinMax(mMin, mMax);
         var w = clampMinMax(wiMin, wiMax);
         var r = clampMinMax(rMin, rMax);
         var s = clampMinMax(sunMin, sunMax);
         var aMinVal = a[0];
         var aMaxVal = a[1];
         var mMinVal = m[0];
         var mMaxVal = m[1];
         var wiMinVal = w[0];
         var wiMaxVal = w[1];
         var rMinVal = r[0];
         var rMaxVal = r[1];
         var sunMinVal = s[0];
         var sunMaxVal = s[1];
         group.clearLayers();
         var visible = 0;
         allMarkers.forEach(function(layer) {
           var meta = layerMeta(layer);
           var blob = (meta.name + ' ' + meta.uid + ' ' + meta.difficulty).toLowerCase();
           if (term && blob.indexOf(term) === -1) return;
           if (diffTerm) {
            var diffBlob = String(meta.difficulty || '').toLowerCase().replace(/\\s+/g, '');
            var diffNeedle = diffTerm.replace(/\\s+/g, '');
             if (diffBlob.indexOf(diffNeedle) === -1) {
               var parsedSearch = parseDifficulty(diffTerm);
               var match = false;
               function sameGrade(v, q){ return isFinite(v) && isFinite(q) && Math.abs(v - q) <= 0.26; }
               if (isFinite(parsedSearch.a) && sameGrade(meta.grades.a, parsedSearch.a)) match = true;
               if (isFinite(parsedSearch.m) && sameGrade(meta.grades.m, parsedSearch.m)) match = true;
               if (isFinite(parsedSearch.wi) && sameGrade(meta.grades.wi, parsedSearch.wi)) match = true;
               if (isFinite(parsedSearch.r) && sameGrade(meta.grades.r, parsedSearch.r)) match = true;
               if (!match) return;
             }
           }
           if (!inRange(meta.grades.a, aMinVal, aMaxVal, aMin, aMax)) return;
           if (!inRange(meta.grades.m, mMinVal, mMaxVal, mMin, mMax)) return;
           if (!inRange(meta.grades.wi, wiMinVal, wiMaxVal, wiMin, wiMax)) return;
           if (!inRange(meta.grades.r, rMinVal, rMaxVal, rMin, rMax)) return;
           if (!inRange(meta.sun, sunMinVal, sunMaxVal, sunMin, sunMax)) return;
             group.addLayer(layer);
             visible += 1;
         });
         var parts = [];
         if (term) parts.push('Suche: ' + term);
         if (diffTerm) parts.push('Grad: ' + diffTerm);
         var aTxt = aRangeTxt ? aRangeTxt.textContent.replace(/[\\t\\n\\r ]+/g, ' ') : ('A' + fmtGrade(aMinVal) + ' – A' + fmtGrade(aMaxVal));
         var mTxt = mRangeTxt ? mRangeTxt.textContent.replace(/[\\t\\n\\r ]+/g, ' ') : ('M' + fmtGrade(mMinVal) + ' – M' + fmtGrade(mMaxVal));
         var wiTxt = wiRangeTxt ? wiRangeTxt.textContent.replace(/[\\t\\n\\r ]+/g, ' ') : ('WI' + fmtGrade(wiMinVal) + ' – WI' + fmtGrade(wiMaxVal));
         var rTxt = rRangeTxt ? rRangeTxt.textContent.replace(/[\\t\\n\\r ]+/g, ' ') : (fmtGrade(rMinVal) + ' – ' + fmtGrade(rMaxVal));
         var sunTxt = sunRangeTxt ? sunRangeTxt.textContent.replace(/[\\t\\n\\r ]+/g, ' ') : (sunMinVal.toFixed(1) + ' – ' + sunMaxVal.toFixed(1) + ' h');
         parts.push(formatRange('A', aTxt, ''));
         parts.push(formatRange('M', mTxt, ''));
         parts.push(formatRange('WI', wiTxt, ''));
         parts.push(formatRange('Fels', rTxt, ''));
         parts.push('Sonne ' + sunTxt);
         status.textContent = visible + ' / ' + allMarkers.length + ' Eisfälle · ' + parts.join(' · ');
       }

       input.addEventListener('input', applyFilter);
       if (diffInput) diffInput.addEventListener('input', applyFilter);
       if (aMin) aMin.addEventListener('input', applyFilter);
       if (aMax) aMax.addEventListener('input', applyFilter);
       if (mMin) mMin.addEventListener('input', applyFilter);
       if (mMax) mMax.addEventListener('input', applyFilter);
       if (wiMin) wiMin.addEventListener('input', applyFilter);
       if (wiMax) wiMax.addEventListener('input', applyFilter);
       if (rMin) rMin.addEventListener('input', applyFilter);
       if (rMax) rMax.addEventListener('input', applyFilter);
       if (sunMin) sunMin.addEventListener('input', applyFilter);
       if (sunMax) sunMax.addEventListener('input', applyFilter);
       if (resetBtn) resetBtn.addEventListener('click', function(){
         resetFilters();
         updateRangeLabels();
         applyFilter();
       });
       resetFilters();
       updateRangeLabels();
       applyFilter();
       if (map && typeof map.on === 'function') {
         map.on('moveend zoomend overlayadd overlayremove', function(){
           ensureGroupVisible('Eisfälle', group);
         });
       }

       if (geoBtn) {
         geoBtn.addEventListener('click', function(){
           if (!navigator.geolocation) {
             if (geoStatus) geoStatus.textContent = 'GPS nicht verfügbar.';
             return;
           }
           if (geoStatus) geoStatus.textContent = 'GPS wird ermittelt...';
           navigator.geolocation.getCurrentPosition(function(pos){
             var lat = pos.coords.latitude;
             var lon = pos.coords.longitude;
             if (geoStatus) geoStatus.textContent = 'GPS: ' + lat.toFixed(4) + ', ' + lon.toFixed(4);
             if (typeof L !== 'undefined') {
               if (!map._geoMarker) {
                 map._geoMarker = L.circleMarker([lat, lon], {
                   radius: 6,
                   color: '#2563eb',
                   weight: 2,
                   fillColor: '#60a5fa',
                   fillOpacity: 0.9
                 }).addTo(map);
               } else {
                 map._geoMarker.setLatLng([lat, lon]);
               }
             }
             if (typeof map.setView === 'function') {
               var nextZoom = Math.max(map.getZoom ? map.getZoom() : 0, 11);
               map.setView([lat, lon], nextZoom);
             }
           }, function(err){
             if (geoStatus) geoStatus.textContent = 'GPS Fehler: ' + (err && err.message ? err.message : 'Unbekannt');
           }, { enableHighAccuracy: true, timeout: 10000 });
         });
       }
    }"
  ) |>
  fitBounds(lng1 = ext@xmin, lat1 = ext@ymin, lng2 = ext@xmax, lat2 = ext@ymax) |>
  addLegend(
    pal       = pal_ci,
    values    = c(0, 1),
    title     = "Climbability",
    labFormat = labelFormat(digits = 1),
    position  = "bottomleft"
  ) |>
  addLegend(
    pal       = pal_h,
    values    = c(0, max_h),
    title     = "Eisdicke (m)",
    labFormat = labelFormat(digits = 2),
    position  = "bottomleft"
  ) |>
  addMarkers(
    data        = marker_data,
    lng         = ~longitude,
    lat         = ~latitude,
    options     = pathOptions(pane = "icefallsPane"),
    clusterOptions = markerClusterOptions(
      showCoverageOnHover = FALSE,
      spiderfyOnMaxZoom = TRUE,
      zoomToBoundsOnClick = TRUE,
      disableClusteringAtZoom = 12
    ),
    popup       = ~popup,
    group       = "Eisfälle"
  )

if (isTRUE(preview_mode)) {
  m <- m |>
    addLayersControl(
      baseGroups    = c("OSM", "Gelände (Topo)"),
      overlayGroups = c("Eisdicke", "Climbability", "Eisfälle"),
      options       = layersControlOptions(collapsed = FALSE)
    )
} else {
  m <- m |>
    addLayersControl(
      baseGroups    = c("OSM", "Gelände (Topo)"),
      overlayGroups = c("Eisfälle"),
      options       = layersControlOptions(collapsed = FALSE)
    )
}

# Zeit-Slider: Steps bleiben 1:1, aber wir wechseln nur die PNG-URL
m <- m |>
  (\(x) {
    if (length(time_labels) > 0L) {
      labels_js  <- paste0("['", paste(time_labels, collapse = "','"), "']")
      n_steps_js <- n_steps
      
      js_code <- sprintf(
        "function(el, x) {
           var map = this;

           var isMobile = window.matchMedia && window.matchMedia('(max-width: 720px)').matches;

           // UI scale
           var lc = el.getElementsByClassName('leaflet-control-layers-expanded')[0]
                    || el.getElementsByClassName('leaflet-control-layers')[0];
           if (lc) {
              if (isMobile) {
                lc.style.marginTop   = '6px';
                lc.style.marginRight = '0';
                lc.style.transform   = 'none';
                lc.style.padding     = '8px 10px';
                lc.style.fontSize    = '13px';
              } else {
                lc.style.marginTop   = '10px';
                lc.style.marginRight = '90px';
                lc.style.transform   = 'scale(1.5)';
                lc.style.transformOrigin = 'top left';
                lc.style.padding     = '12px 15px';
                lc.style.fontSize    = '16px';
              }
           }

           var labels = %s;
           var nSteps = %d;
           if (!labels || labels.length === 0 || nSteps <= 0) return;

           function pad3(n){ return String(n).padStart(3,'0'); }

           var iceLayer   = window._iceOverlay   || null;
           var climbLayer = window._climbOverlay || null;

           if (!iceLayer || !climbLayer) {
             map.eachLayer(function(l){
               if(!iceLayer   && l && l.options && l.options.layerId === 'ice_overlay')   iceLayer = l;
               if(!climbLayer && l && l.options && l.options.layerId === 'climb_overlay') climbLayer = l;
             });
           }

           var initial = nSteps - 1;
           if (initial < 0) initial = 0;

           function setTimeStep(step) {
             if (step < 0) step = 0;
             if (step >= nSteps) step = nSteps - 1;

             var i = step + 1; // 1..nSteps
             if (iceLayer)   iceLayer.setUrl('img/ice_' + pad3(i) + '.png');
             if (climbLayer) climbLayer.setUrl('img/climb_' + pad3(i) + '.png');

             var labelDiv = document.getElementById('time-label');
             if (labelDiv && step >= 0 && step < labels.length) {
               labelDiv.textContent = labels[step];
             }

             var next = Math.min(nSteps, i+1);
             var prev = Math.max(1, i-1);
             var img1 = new Image(); img1.src = 'img/ice_' + pad3(next) + '.png';
             var img2 = new Image(); img2.src = 'img/climb_' + pad3(next) + '.png';
             var img3 = new Image(); img3.src = 'img/ice_' + pad3(prev) + '.png';
             var img4 = new Image(); img4.src = 'img/climb_' + pad3(prev) + '.png';
           }

           var sliderControl = L.control({position: 'topright'});
           sliderControl.onAdd = function() {
             var div = L.DomUtil.create('div', 'info leaflet-control');
             div.style.background   = 'rgba(255,255,255,0.9)';
             div.style.padding      = '8px 10px';
             div.style.borderRadius = '6px';
             div.style.minWidth     = isMobile ? '200px' : '260px';

             // Mehr Abstand zur Layer-Control
             div.style.marginTop    = isMobile ? '110px' : '140px';
             div.style.marginRight  = isMobile ? '6px' : '10px';

             var title = document.createElement('div');
             title.style.fontSize    = '16px';
             title.style.marginBottom = '4px';
             title.innerHTML = '<b>Eisdicke & Climbability – Zeitverlauf</b>';
             div.appendChild(title);

             var labelDiv = document.createElement('div');
             labelDiv.id = 'time-label';
             labelDiv.style.fontSize   = '14px';
             labelDiv.style.marginBottom = '4px';
             labelDiv.textContent = labels[initial] || labels[0];
             div.appendChild(labelDiv);

             var slider = document.createElement('input');
             slider.type  = 'range';
             slider.min   = 0;
             slider.max   = nSteps - 1;
             slider.step  = 1;
             slider.value = initial;
             slider.style.width = isMobile ? '200px' : '240px';
             slider.id    = 'time-slider';
             div.appendChild(slider);

             slider.addEventListener('input', function(e) {
               var step = parseInt(e.target.value, 10);
               if (!isNaN(step)) setTimeStep(step);
             });

             slider.addEventListener('mousedown', function() { if (map && map.dragging) map.dragging.disable(); });
             slider.addEventListener('mouseup',   function() { if (map && map.dragging) map.dragging.enable();  });

             L.DomEvent.disableClickPropagation(div);
             return div;
           };
           sliderControl.addTo(map);

           setTimeout(function(){ setTimeStep(initial); }, 200);
         }",
        labels_js,
        n_steps_js
      )
      
      htmlwidgets::onRender(x, js_code)
    } else {
      x
    }
  })()

# 13) Output: NICHT selfcontained, in site/ ----------------------------

dir.create("site", showWarnings = FALSE)
saveWidget(m, "site/map.html", selfcontained = FALSE)
message("✅ Fertig: site/map.html + site/img/*.png")

if (interactive()) {
  m
}
