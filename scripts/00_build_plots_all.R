suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
})

assign_path <- "data/AWS/icefalls_nearest_station.csv"
if (!file.exists(assign_path)) stop("Fehlt: ", assign_path)

assign <- read_csv(assign_path, show_col_types = FALSE)

if (!("uid" %in% names(assign))) stop("Spalte 'uid' fehlt in ", assign_path)

uids <- sort(unique(assign$uid))
uids <- uids[is.finite(uids)]

if (length(uids) == 0) stop("Keine UIDs gefunden.")

# Optional: limit build to a specific UID list (for faster testing)
# Usage:
#   Rscript scripts/00_build_plots_all.R --uids=12,34,56
#   or set environment variable UID_LIMIT="12,34,56"
uid_arg <- grep("^--uids=", commandArgs(trailingOnly = TRUE), value = TRUE)
uid_raw <- if (length(uid_arg) > 0) sub("^--uids=", "", uid_arg[1]) else Sys.getenv("UID_LIMIT", "")
if (nzchar(uid_raw)) {
  uid_list <- suppressWarnings(as.integer(trimws(unlist(strsplit(uid_raw, "[,;\\s]+")))))
  uid_list <- uid_list[is.finite(uid_list)]
  if (length(uid_list) == 0) stop("UID_LIMIT/--uids enthaelt keine gueltigen UIDs.")
  uids <- uids[uids %in% uid_list]
  if (length(uids) == 0) stop("Keine UIDs uebrig nach Filter: ", uid_raw)
  message("⚙️ UID-Filter aktiv: ", paste(uids, collapse = ", "))
}

dir.create("site/plots", recursive = TRUE, showWarnings = FALSE)

# 0) Inversion einmal berechnen (Cache)
inv_script <- "scripts/00_build_inversion_cache.R"
if (!file.exists(inv_script)) stop("Fehlt Inversion-Skript: ", inv_script)

system2("Rscript", c(inv_script), stdout = "", stderr = "")

# Diagramm-Skript Pfad (anpassen, wie du es ablegst)
plot_script <- "scripts/diagram_uid.R"
if (!file.exists(plot_script)) stop("Fehlt Diagramm-Skript: ", plot_script)

failed <- integer(0)

for (u in uids) {
  message("\n==============================")
  message("📈 Build Plot UID: ", u)
  message("==============================")
  ok <- TRUE
  tryCatch({
    # neuer Prozess pro UID (robuster als source())
    system2("Rscript", c(plot_script, as.character(u)), stdout = "", stderr = "")
  }, error = function(e) {
    ok <<- FALSE
    message("❌ UID ", u, " fehlgeschlagen: ", e$message)
  })
  if (!ok) failed <- c(failed, u)
}

if (length(failed) > 0) {
  stop("Plots fehlgeschlagen für UIDs: ", paste(failed, collapse = ", "))
}

message("\n✅ Alle Plots gebaut: ", length(uids))
