suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(sf)
})

PATH_META <- "data/Koordinaten_Wasserfaelle/eisklettern_links_entries_diff.csv"
PATH_BUILD <- "analysis/icefall_structure/03_build_icefall_structure.R"
OUT_DIR <- "data/derived/icefall_structure"
DEFAULT_BATCH_DIR <- "tmp/icefall_structure_batches"

args <- commandArgs(trailingOnly = TRUE)
chunk_arg <- grep("^--chunk-size=", args, value = TRUE)
batch_arg <- grep("^--batch-dir=", args, value = TRUE)
force_cache <- any(args == "--force")

chunk_size <- if (length(chunk_arg) > 0) {
  suppressWarnings(as.integer(sub("^--chunk-size=", "", chunk_arg[[1]])))
} else {
  90L
}
if (!is.finite(chunk_size) || chunk_size < 1L) {
  stop("Invalid --chunk-size value.")
}

batch_dir <- if (length(batch_arg) > 0) {
  sub("^--batch-dir=", "", batch_arg[[1]])
} else {
  DEFAULT_BATCH_DIR
}

detect_delim <- function(path) {
  header <- readLines(path, n = 1, warn = FALSE)
  if (length(header) == 0) stop("Empty file: ", path)
  counts <- c(
    ";" = stringr::str_count(header, stringr::fixed(";")),
    "," = stringr::str_count(header, stringr::fixed(",")),
    "\t" = stringr::str_count(header, stringr::fixed("\t"))
  )
  names(counts)[which.max(counts)]
}

read_any_csv <- function(path) {
  readr::read_delim(
    file = path,
    delim = detect_delim(path),
    col_types = readr::cols(.default = readr::col_character()),
    show_col_types = FALSE,
    progress = FALSE
  )
}

parse_uid <- function(x) {
  as.integer(readr::parse_number(as.character(x)))
}

rscript_bin <- file.path(
  R.home("bin"),
  if (.Platform$OS.type == "windows") "Rscript.exe" else "Rscript"
)
if (!file.exists(rscript_bin)) stop("Could not locate Rscript binary: ", rscript_bin)
if (!file.exists(PATH_BUILD)) stop("Missing build script: ", PATH_BUILD)

meta <- read_any_csv(PATH_META)
uids <- sort(unique(parse_uid(meta$uid)))
uids <- uids[is.finite(uids)]
if (length(uids) == 0) stop("No UIDs found in meta file.")

if (dir.exists(batch_dir)) unlink(batch_dir, recursive = TRUE, force = TRUE)
dir.create(batch_dir, recursive = TRUE, showWarnings = FALSE)

chunks <- split(uids, ceiling(seq_along(uids) / chunk_size))
analysis_files <- character(length(chunks))
qa_files <- character(length(chunks))
route_files <- character(length(chunks))

for (i in seq_along(chunks)) {
  chunk <- chunks[[i]]
  batch_id <- sprintf("%02d", i)
  uid_arg <- paste0("--uids=", paste(chunk, collapse = ","))
  cmd_args <- c(PATH_BUILD, uid_arg)
  if (force_cache) cmd_args <- c(cmd_args, "--force")

  message(
    "Batch ", batch_id, "/", sprintf("%02d", length(chunks)),
    ": uid ", chunk[[1]], "-", chunk[[length(chunk)]]
  )

  exit_code <- system2(rscript_bin, cmd_args)
  if (!identical(exit_code, 0L)) {
    stop("Batch failed for ", uid_arg)
  }

  analysis_files[[i]] <- file.path(batch_dir, paste0("analysis_", batch_id, ".csv"))
  qa_files[[i]] <- file.path(batch_dir, paste0("qa_", batch_id, ".csv"))
  route_files[[i]] <- file.path(batch_dir, paste0("routes_", batch_id, ".geojson"))

  ok <- file.copy(
    from = c(
      file.path(OUT_DIR, "icefall_structure_analysis.csv"),
      file.path(OUT_DIR, "icefall_structure_qa.csv"),
      file.path(OUT_DIR, "icefall_routes.geojson")
    ),
    to = c(analysis_files[[i]], qa_files[[i]], route_files[[i]]),
    overwrite = TRUE
  )
  if (!all(ok)) {
    stop("Failed to copy batch outputs for batch ", batch_id)
  }
}

analysis <- bind_rows(lapply(analysis_files, readr::read_csv, show_col_types = FALSE)) %>%
  arrange(uid) %>%
  distinct(uid, .keep_all = TRUE)

qa <- bind_rows(lapply(qa_files, readr::read_csv, show_col_types = FALSE)) %>%
  arrange(uid) %>%
  distinct(uid, .keep_all = TRUE)

routes <- do.call(rbind, lapply(route_files, function(path) suppressWarnings(sf::st_read(path, quiet = TRUE)))) %>%
  arrange(uid) %>%
  distinct(uid, .keep_all = TRUE)

readr::write_csv(analysis, file.path(OUT_DIR, "icefall_structure_analysis.csv"), na = "")
readr::write_csv(qa, file.path(OUT_DIR, "icefall_structure_qa.csv"), na = "")
suppressWarnings(
  sf::st_write(routes, file.path(OUT_DIR, "icefall_routes.geojson"), quiet = TRUE, delete_dsn = TRUE)
)

message("Merged analysis rows: ", nrow(analysis))
message("Merged QA rows: ", nrow(qa))
message("Merged route features: ", nrow(routes))
