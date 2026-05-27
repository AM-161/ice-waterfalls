suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
})

assign_path <- "data/AWS/icefalls_nearest_station.csv"
if (!file.exists(assign_path)) stop("Missing: ", assign_path)

assign <- read_csv(assign_path, show_col_types = FALSE)

if (!("uid" %in% names(assign))) stop("Column 'uid' is missing in ", assign_path)

uids <- sort(unique(assign$uid))
uids <- uids[is.finite(uids)]

if (length(uids) == 0) stop("No UIDs found.")

# Optional: limit build to a specific UID list (for faster testing)
# Usage:
#   Rscript scripts/00_build_plots_all.R --uids=12,34,56
#   or set environment variable UID_LIMIT="12,34,56"
uid_arg <- grep("^--uids=", commandArgs(trailingOnly = TRUE), value = TRUE)
uid_raw <- if (length(uid_arg) > 0) sub("^--uids=", "", uid_arg[1]) else Sys.getenv("UID_LIMIT", "")
if (nzchar(uid_raw)) {
  uid_list <- suppressWarnings(as.integer(trimws(unlist(strsplit(uid_raw, "[,;\\s]+")))))
  uid_list <- uid_list[is.finite(uid_list)]
  if (length(uid_list) == 0) stop("UID_LIMIT/--uids contains no valid UIDs.")
  uids <- uids[uids %in% uid_list]
  if (length(uids) == 0) stop("No UIDs left after filter: ", uid_raw)
  message("UID filter active: ", paste(uids, collapse = ", "))
}

dir.create("site/plots", recursive = TRUE, showWarnings = FALSE)

run_rscript_checked <- function(script, args = character()) {
  status <- system2("Rscript", c(script, args), stdout = "", stderr = "")
  exit_code <- attr(status, "status")
  if (is.null(exit_code)) exit_code <- status
  if (length(exit_code) == 0 || is.na(exit_code)) exit_code <- 0
  if (!identical(as.integer(exit_code), 0L)) {
    stop("Rscript failed with exit code ", exit_code, ": ", script)
  }
  invisible(TRUE)
}

# 0) Build inversion once (cache)
inv_script <- "scripts/00_build_inversion_cache.R"
if (!file.exists(inv_script)) stop("Missing inversion script: ", inv_script)

run_rscript_checked(inv_script)

# Plot script path
plot_script <- "scripts/diagram_uid.R"
if (!file.exists(plot_script)) stop("Missing chart script: ", plot_script)

failed <- integer(0)

for (u in uids) {
  message("\n==============================")
  message("📈 Build Plot UID: ", u)
  message("==============================")
  ok <- TRUE
  tryCatch({
    # New process per UID (more robust than source()).
    run_rscript_checked(plot_script, as.character(u))
  }, error = function(e) {
    ok <<- FALSE
    message("UID ", u, " failed: ", e$message)
  })
  if (!ok) failed <- c(failed, u)
}

if (length(failed) > 0) {
  stop("Plots failed for UIDs: ", paste(failed, collapse = ", "))
}

message("\nAll plots built: ", length(uids))
