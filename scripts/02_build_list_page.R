# scripts/02_build_list_page.R
# ============================================================
# Build list page (summary table) for GitHub Pages + offline viewing
# - meta:        data/Koordinaten_Wasserfaelle/tirol_eisklettern_links_entries_diff.csv
# - assignments: data/AWS/icefalls_nearest_station.csv (optional)
# - sun:         data/Koordinaten_Wasserfaelle/icefalls_sun_horizon.csv (optional)
# - model runs:  data/ModelRuns/model_uid<uid>.csv
# - outputs:     site/icefalls_table.json + site/list.html
#   plus copies to repo root: icefalls_table.json + list.html
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(lubridate)
  library(jsonlite)
  library(tibble)
})

TZ_LOCAL <- "Europe/Vienna"

# Tomorrow in local TZ (used for sun + model summaries)
tomorrow <- as.Date(with_tz(Sys.time(), TZ_LOCAL) + days(1))

# ----------------------------
# Paths
# ----------------------------
PATH_ASSIGN <- "data/AWS/icefalls_nearest_station.csv"
PATH_META   <- "data/Koordinaten_Wasserfaelle/tirol_eisklettern_links_entries_diff.csv"
PATH_SUN    <- "data/Koordinaten_Wasserfaelle/icefalls_sun_horizon.csv"
DIR_MODELS  <- "data/ModelRuns"

OUT_DIR  <- "site"
OUT_JSON <- file.path(OUT_DIR, "icefalls_table.json")
OUT_HTML <- file.path(OUT_DIR, "list.html")

ROOT_JSON <- "icefalls_table.json"
ROOT_HTML <- "list.html"

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ----------------------------
# Helpers
# ----------------------------
parse_uid <- function(x) {
  as.integer(readr::parse_number(as.character(x)))
}

to_num <- function(x) {
  if (is.null(x)) return(NA_real_)
  if (is.numeric(x)) return(x)
  x <- as.character(x)
  x[x %in% c("", "NA", "NaN", "NULL")] <- NA_character_
  x <- gsub(",", ".", x, fixed = TRUE)
  suppressWarnings(as.numeric(x))
}

read_any_delim <- function(path) {
  x <- tryCatch(readr::read_delim(path, delim = "\t", show_col_types = FALSE, progress = FALSE), error = function(e) NULL)
  if (!is.null(x) && ncol(x) > 1) return(x)
  x <- tryCatch(readr::read_delim(path, delim = ";", show_col_types = FALSE, progress = FALSE), error = function(e) NULL)
  if (!is.null(x) && ncol(x) > 1) return(x)
  readr::read_csv(path, show_col_types = FALSE, progress = FALSE)
}

get_chr <- function(df, ...) {
  cands <- c(...)
  for (nm in cands) if (nm %in% names(df)) return(as.character(df[[nm]]))
  rep(NA_character_, nrow(df))
}

get_num <- function(df, ...) {
  cands <- c(...)
  for (nm in cands) if (nm %in% names(df)) return(to_num(df[[nm]]))
  rep(NA_real_, nrow(df))
}

first_nonempty <- function(x) {
  x <- as.character(x)
  x[x %in% c("", "NA", "NaN", "NULL")] <- NA_character_
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA_character_)
  x[[1]]
}

parse_time_any <- function(x, tz = TZ_LOCAL) {
  if (inherits(x, "POSIXct")) return(with_tz(x, tz))
  x <- as.character(x)
  x[x %in% c("", "NA", "NaN", "NULL")] <- NA_character_
  
  # handle ISO strings like 2025-11-01T13:13:00Z
  x <- sub("Z$", "", x)
  x <- gsub("T", " ", x, fixed = TRUE)
  
  t <- suppressWarnings(lubridate::ymd_hms(x, tz = tz))
  if (all(is.na(t))) t <- suppressWarnings(lubridate::ymd_hm(x, tz = tz))
  if (all(is.na(t))) t <- suppressWarnings(lubridate::parse_date_time(
    x,
    orders = c("Ymd HMS", "Ymd HM", "Y-m-d H:M:S", "Y-m-d H:M", "Y-m-dTH:M:S", "Y-m-dTH:M"),
    tz = tz
  ))
  t
}

parse_time_iso_z <- function(x, out_tz = TZ_LOCAL) {
  if (is.null(x)) return(as.POSIXct(NA))
  x <- as.character(x)
  x[x %in% c("", "NA", "NaN", "NULL")] <- NA_character_
  x2 <- sub("Z$", "", x)
  x2 <- gsub("T", " ", x2, fixed = TRUE)
  t <- suppressWarnings(lubridate::ymd_hms(x2, tz = "UTC"))
  if (all(is.na(t))) t <- suppressWarnings(lubridate::ymd_hm(x2, tz = "UTC"))
  suppressWarnings(lubridate::with_tz(t, out_tz))
}

fmt_num <- function(x, digits = 2) {
  ifelse(is.finite(x), formatC(x, format = "f", digits = digits), NA_character_)
}

fmt_pct <- function(x, digits = 0) {
  ifelse(is.finite(x), paste0(round(x * 100, digits), "%"), NA_character_)
}

fmt_hm <- function(t) {
  ifelse(!is.na(t), format(t, "%H:%M"), NA_character_)
}

fmt_duration_h <- function(h) {
  h <- to_num(h)
  out <- rep(NA_character_, length(h))
  ok <- is.finite(h)
  mins <- round(h[ok] * 60)
  hh <- mins %/% 60
  mm <- mins %% 60
  out[ok] <- paste0(hh, " h ", mm, " min")
  out
}

# Fix common encoding / symbol issues (e.g., degree sign)
normalize_text <- function(x) {
  if (is.null(x)) return(NA_character_)
  x <- as.character(x)
  # best-effort to UTF-8 (no-op if already UTF-8)
  x <- suppressWarnings(iconv(x, from = "", to = "UTF-8"))
  # normalize degree symbol variants / mojibake
  x <- gsub("Â°", "°", x, fixed = TRUE)
  x <- gsub("º",  "°", x, fixed = TRUE)
  x <- gsub("ø",  "°", x, fixed = TRUE)
  x
}


# ----------------------------
# 1) Load meta (CSV)
# ----------------------------
if (!file.exists(PATH_META)) stop("Fehlt: ", PATH_META)

meta_raw <- read_any_delim(PATH_META) %>%
  rename_with(tolower)

if (!"uid" %in% names(meta_raw)) stop("META CSV hat keine Spalte 'uid'.")

meta <- tibble(
  uid = parse_uid(meta_raw$uid),
  name = get_chr(meta_raw, "name"),
  latitude  = get_num(meta_raw, "latitude", "lat"),
  longitude = get_num(meta_raw, "longitude", "lon"),
  elev_m = get_num(meta_raw, "hoehe_dgm5m", "hoehe", "höhe", "elevation", "elev_m"),
  difficulty = get_chr(meta_raw, "schwierigkeit", "difficulty", "grad"),
  icefall_height_m = get_num(meta_raw, "eisfallhhe", "eisfallhoehe", "eisfallhöhe", "height_m", "icefall_height_m"),
  aspect = get_chr(meta_raw, "ausrichtung", "aspect"),
  approach = get_chr(meta_raw, "zustieg", "approach"),
  descent  = get_chr(meta_raw, "abstieg", "descent"),
  first_ascent = get_chr(meta_raw, "erstbegehnung", "first_ascent"),
  description  = get_chr(meta_raw, "beschreibung", "description")
) %>%
  filter(!is.na(uid)) %>%
  mutate(dplyr::across(where(is.character), normalize_text))

uid_raw <- as.character(meta_raw$uid)
uid_parsed <- parse_uid(uid_raw)
message(
  "META: rows=", nrow(meta_raw),
  " | parsed rows=", nrow(meta),
  " | unique uids=", length(unique(meta$uid)),
  " | uid NA=", sum(is.na(uid_parsed)), "/", length(uid_parsed)
)
message("META uid examples: ", paste(head(uid_raw, 8), collapse = " | "))
if (nrow(meta) <= 1) {
  warning(
    "META parsed to ", nrow(meta), " row(s).",
    " This often means PATH_META is a diff/partial file or uid parsing failed."
  )
}

# ----------------------------
# 2) Load assign (optional)
# ----------------------------
assign <- NULL
if (file.exists(PATH_ASSIGN)) {
  assign <- read_any_delim(PATH_ASSIGN) %>%
    mutate(uid = parse_uid(uid))
}

# ----------------------------
# 3) Sun horizons (optional)
# ----------------------------
sun <- NULL
if (file.exists(PATH_SUN)) {
  sun_raw <- read_any_delim(PATH_SUN) %>%
    rename_with(tolower) %>%
    mutate(
      uid = parse_uid(uid),
      date = as.Date(get_chr(., "date"))
    )
  
  sun <- sun_raw %>%
    filter(.data$date == tomorrow) %>%
    group_by(uid) %>%
    summarise(
      topo_url  = dplyr::coalesce(first(topo_url[topo_url != ""]), first(topo_url)),
      topo_slug = dplyr::coalesce(first(topo_slug[topo_slug != ""]), first(topo_slug)),
      sunrise_topo_local = parse_time_iso_z(first_nonempty(sunrise_topo), out_tz = TZ_LOCAL),
      sunset_topo_local  = parse_time_iso_z(first_nonempty(sunset_topo),  out_tz = TZ_LOCAL),
      sun_hours_tomorrow_h = to_num(first_nonempty(sun_hours_topo)),
      sun_tomorrow_range_txt = dplyr::if_else(
        is.na(sunrise_topo_local) | is.na(sunset_topo_local),
        NA_character_,
        paste0(fmt_hm(sunrise_topo_local), "-", fmt_hm(sunset_topo_local))
      ),
      sun_duration_tomorrow_txt = fmt_duration_h(sun_hours_tomorrow_h),
      .groups = "drop"
    )
}

# ----------------------------
# 4) Model summary (tomorrow)
# ----------------------------
summarise_uid_model <- function(uid) {
  f <- file.path(DIR_MODELS, sprintf("model_uid%s.csv", uid))
  
  empty <- tibble(
    uid = uid,
    thickness_tomorrow_07_m = NA_real_,
    climb_max_tomorrow = NA_real_,
    climb_max_time_local = NA_character_,
    thickness_at_climb_max_m = NA_real_
  )
  
  if (!file.exists(f) || file.info(f)$size <= 0) return(empty)
  
  df <- tryCatch(readr::read_csv(f, show_col_types = FALSE, progress = FALSE), error = function(e) NULL)
  if (is.null(df) || !"time" %in% names(df)) return(empty)
  
  df <- df %>%
    mutate(
      time = parse_time_any(.data$time, tz = TZ_LOCAL),
      date = as.Date(time),
      thickness_m  = if ("thickness_m" %in% names(df)) to_num(.data$thickness_m) else NA_real_,
      climbability = if ("climbability" %in% names(df)) to_num(.data$climbability) else NA_real_
    ) %>%
    filter(!is.na(time))
  
  df_day <- df %>% filter(date == tomorrow)
  if (nrow(df_day) == 0) return(empty)
  
  # thickness at ~07:00 local (closest)
  t07 <- as.POSIXct(paste0(format(tomorrow, "%Y-%m-%d"), " 07:00:00"), tz = TZ_LOCAL)
  i07 <- which.min(abs(as.numeric(difftime(df_day$time, t07, units = "mins"))))
  thickness_07 <- df_day$thickness_m[i07]
  
  if (all(!is.finite(df_day$climbability))) {
    climb_max <- NA_real_
    climb_time <- NA_character_
    thick_at_best <- NA_real_
  } else {
    imax <- which.max(df_day$climbability)
    climb_max <- df_day$climbability[imax]
    climb_time <- format(df_day$time[imax], "%d.%m.%Y %H:%M")
    thick_at_best <- df_day$thickness_m[imax]
  }
  
  tibble(
    uid = uid,
    thickness_tomorrow_07_m = thickness_07,
    climb_max_tomorrow = climb_max,
    climb_max_time_local = climb_time,
    thickness_at_climb_max_m = thick_at_best
  )
}

uids <- sort(unique(meta$uid))
model_sum <- bind_rows(lapply(uids, summarise_uid_model))

# ----------------------------
# 5) Merge
# ----------------------------
out <- meta %>% left_join(model_sum, by = "uid")

if (!is.null(assign)) {
  assign_slim <- assign %>%
    dplyr::select(dplyr::any_of(c(
      "uid", "station_id", "source", "dist_km", "elev_diff_m",
      "icefall_name", "ice_lon", "ice_lat", "icefall_elev_m", "icefall_height_m"
    ))) %>%
    dplyr::mutate(uid = parse_uid(uid))
  
  out <- out %>%
    left_join(assign_slim, by = "uid") %>%
    mutate(
      name = coalesce(as.character(.data$icefall_name), .data$name, paste0("UID ", .data$uid)),
      latitude  = coalesce(to_num(.data$ice_lat), .data$latitude),
      longitude = coalesce(to_num(.data$ice_lon), .data$longitude),
      elev_m = coalesce(to_num(.data$icefall_elev_m), .data$elev_m),
      icefall_height_m = coalesce(to_num(.data$icefall_height_m), .data$icefall_height_m)
    )
} else {
  out$station_id <- NA_character_
  out$source <- NA_character_
  out$dist_km <- NA_real_
  out$elev_diff_m <- NA_real_
}

if (!is.null(sun)) {
  out <- out %>% left_join(sun, by = "uid")
} else {
  out$topo_url <- NA_character_
  out$topo_slug <- NA_character_
  out$sun_tomorrow_range_txt <- NA_character_
  out$sun_hours_tomorrow_h <- NA_real_
  out$sun_duration_tomorrow_txt <- NA_character_
}

out <- out %>%
  mutate(
    plot_url = sprintf("plots/uid_%03d.png", uid),
    thickness_tomorrow_07_txt = fmt_num(thickness_tomorrow_07_m, 2),
    climb_max_tomorrow_txt    = fmt_pct(climb_max_tomorrow, 0),
    thickness_at_best_txt     = fmt_num(thickness_at_climb_max_m, 2)
  ) %>%
  arrange(desc(climb_max_tomorrow), desc(thickness_tomorrow_07_m))

# ----------------------------
# 6) Write JSON
# ----------------------------
jsonlite::write_json(out, OUT_JSON, pretty = TRUE, auto_unbox = TRUE, na = "null")
message("✅ Wrote JSON: ", OUT_JSON)

# ----------------------------
# 7) Write list.html
#    Important offline fix:
#    - Use Base64-embedded JSON (no fetch needed for file://)
# ----------------------------
tom_str <- format(tomorrow, "%d.%m.%Y")

embedded_json <- jsonlite::toJSON(out, auto_unbox = TRUE, na = "null")
embedded_b64  <- jsonlite::base64_enc(charToRaw(enc2utf8(embedded_json)))

# Build HTML as lines to avoid locale/size parser limits (notably on Windows)
# and to keep the R source ASCII-only (umlauts via HTML entities).
html_lines <- c(
  '<!doctype html>',
  '<html lang="de">',
  '<head>',
  '  <meta charset="utf-8"/>',
  '  <meta name="viewport" content="width=device-width, initial-scale=1"/>',
  '  <title>Icefalls - &Uuml;bersicht</title>',
  '  <style>',
  '    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; padding: 0; }',
  '    header { padding: 10px 14px; border-bottom: 1px solid #ddd; display:flex; gap:12px; align-items:center; flex-wrap:wrap; }',
  '    header a { text-decoration:none; padding:6px 10px; border:1px solid #ddd; border-radius:8px; color:#111; }',
  '    header a:hover { background:#f4f4f4; }',
  '    .wrap { padding: 12px 14px; }',
  '    .controls { display:flex; gap:10px; flex-wrap:wrap; align-items:center; margin-bottom:10px; }',
  '    input[type="search"], input[type="number"], select { padding:10px 12px; border:1px solid #ccc; border-radius:10px; font-size:16px; }',
  '    input[type="range"] { width: 220px; }',
  '    table { width:100%; border-collapse: collapse; }',
  '    th, td { padding: 10px 8px; border-bottom: 1px solid #eee; vertical-align: top; }',
  '    th { text-align:left; position: sticky; top: 0; background: #fff; z-index: 1; cursor:pointer; user-select:none; }',
  '    tr:hover { background: #fafafa; }',
  '    .muted { color:#666; font-size:12px; }',
  '    .btn { display:inline-flex; gap:6px; align-items:center; padding:6px 10px; border:1px solid #ddd; border-radius:10px; background:#fff; cursor:pointer; }',
  '    .btn:hover { background:#f4f4f4; }',
  '    .small { font-size: 12px; }',
  '    details > summary { list-style: none; }',
  '    details > summary::-webkit-details-marker { display:none; }',
  '    #filters .panel { margin-top:10px; padding:10px; border:1px solid #eee; border-radius:12px; background:#fafafa; display:flex; flex-direction:column; gap:10px; }',
  '    #filters .row { display:flex; flex-wrap:wrap; gap:10px; align-items:center; }',
  '    #filters label { font-size:12px; color:#666; min-width:140px; }',
  '    #filters input[type="text"] { min-width:240px; }',
  '    #modal { display:none; position:fixed; inset:0; background:rgba(0,0,0,0.8); z-index:9999; }',
  '    #modal .inner { position:absolute; inset:0; display:flex; flex-direction:column; }',
  '    #modal .bar { padding:10px; display:flex; gap:10px; align-items:center; justify-content:space-between; color:#fff; }',
  '    #modal img { flex:1; width:100%; height:100%; object-fit: contain; }',
  '    #modal .bar button, #modal .bar a {',
  '      color:#fff; border:1px solid rgba(255,255,255,0.35);',
  '      background: transparent; padding:8px 12px; border-radius:10px; cursor:pointer;',
  '      text-decoration:none;',
  '    }',
  '    #modal .bar button:hover, #modal .bar a:hover { background: rgba(255,255,255,0.12); }',
  '    @media (max-width: 720px) {',
  '      th, td { padding: 12px 6px; }',
  '      input[type="search"] { width: 100%; min-width: 0; }',
  '      header { gap:8px; }',
  '      input[type="range"] { width: 160px; }',
  '    }',
  '  </style>',
  '</head>',
  '<body>',
  '  <header>',
  '    <a href="index.html">Karte</a>',
  '    <a href="list.html"><b>&Uuml;bersicht</b></a>',
  paste0('    <span class="muted">Morgen: ', tom_str, ' (TZ: Europe/Vienna)</span>'),
  '  </header>',
  '',
  '  <div class="wrap">',
  '    <div class="controls">',
  '      <input id="q" type="search" placeholder="Suchen: Name, Schwierigkeit, Ausrichtung, Station ...">',
  '',
  '      <details id="filters">',
  '        <summary class="btn" type="button">Filter</summary>',
  '        <div class="panel">',
  '          <div class="row">',
  '            <label for="radiusKm">Umkreis</label>',
  '            <input id="radiusKm" type="number" min="0" step="1" value="0" style="width:110px;" title="Radius in km (0 = aus)">',
  '            <span class="muted">0 km = kein Filter</span>',
  '          </div>',
  '',
  '          <div class="row">',
  '            <label>Zentrum</label>',
  '            <button class="btn" id="useGeo" type="button">GPS</button>',
  '            <input id="place" type="text" list="placeSuggestions" placeholder="Ort eingeben (z.B. Obergurgl)">',
  '            <datalist id="placeSuggestions"></datalist>',
  '            <button class="btn" id="geocodeBtn" type="button">Suchen</button>',
  '          </div>',
  '',
  '          <div class="row">',
  '            <label>Koord.</label>',
  '            <input id="centerLat" type="number" step="0.000001" placeholder="lat" style="width:140px;">',
  '            <input id="centerLon" type="number" step="0.000001" placeholder="lon" style="width:140px;">',
  '            <button class="btn" id="setCustom" type="button">Setzen</button>',
  '          </div>',
  '',
  '          <div class="row">',
  '            <label>Technisches Klettern</label>',
  '            <input id="aMin" type="range" min="0.75" max="4.25" step="0.25" value="0.75" title="A1- bis A4+">',
  '            <input id="aMax" type="range" min="0.75" max="4.25" step="0.25" value="4.25" title="A1- bis A4+">',
  '            <span class="muted" id="aRangeTxt">A1- – A4+</span>',
  '          </div>',
  '          <div class="row">',
  '            <label>Mixed (M)</label>',
  '            <input id="mMin" type="range" min="0.75" max="13.25" step="0.25" value="0.75" title="M1- bis M13+">',
  '            <input id="mMax" type="range" min="0.75" max="13.25" step="0.25" value="13.25" title="M1- bis M13+">',
  '            <span class="muted" id="mRangeTxt">M1- – M13+</span>',
  '          </div>',
  '          <div class="row">',
  '            <label>Wassereis (WI)</label>',
  '            <input id="wiMin" type="range" min="0.75" max="7.25" step="0.25" value="0.75" title="WI1- bis WI7+">',
  '            <input id="wiMax" type="range" min="0.75" max="7.25" step="0.25" value="7.25" title="WI1- bis WI7+">',
  '            <span class="muted" id="wiRangeTxt">WI1- – WI7+</span>',
  '          </div>',
  '          <div class="row">',
  '            <label>Fels (UIAA)</label>',
  '            <input id="rMin" type="range" min="0.75" max="12.25" step="0.25" value="0.75" title="1- bis 12+">',
  '            <input id="rMax" type="range" min="0.75" max="12.25" step="0.25" value="12.25" title="1- bis 12+">',
  '            <span class="muted" id="rRangeTxt">1- – 12+</span>',
  '          </div>',
  '          <div class="row">',
  '            <label>Sonne morgen (h)</label>',
  '            <input id="sunMin" type="range" min="0" max="12" step="0.25" value="0" title="Sonnendauer (Topographie)">',
  '            <input id="sunMax" type="range" min="0" max="12" step="0.25" value="12" title="Sonnendauer (Topographie)">',
  '            <span class="muted" id="sunRangeTxt">0.0 – 12.0 h</span>',
  '          </div>',
  '',
  '          <div class="muted small">Hinweis: GPS meist nur ueber https/localhost. Ortsuche benoetigt Internet.</div>',
  '          <div class="muted small" id="geoStatus"></div>',
  '        </div>',
  '      </details>',
  '',
  '      <span class="muted" style="margin-left:auto;">Klick auf Spaltenkopf = sortieren</span>',
  '    </div>',
  '',
  '    <div class="muted small" id="status">Lade Daten ...</div>',
  '',
  '    <table id="tbl">',
  '      <thead>',
  '        <tr>',
  '          <th data-key="name">Eisfall</th>',
  '          <th data-key="difficulty">Schwierigkeit</th>',
  '          <th data-key="_grade_a">Technisches Klettern</th>',
  '          <th data-key="_grade_m">M</th>',
  '          <th data-key="_grade_wi">WI</th>',
  '          <th data-key="_grade_r">Fels</th>',
  '          <th data-key="elev_m">H&ouml;he (m)</th>',
  '          <th data-key="_dist_km">Distanz (km)</th>',
  '          <th data-key="sun_tomorrow_range_txt">Sonne morgen</th>',
  '          <th data-key="sun_hours_tomorrow_h">Sonnendauer</th>',
  '          <th data-key="thickness_tomorrow_07_m">Eisdicke morgen ~07:00 (m)</th>',
  '          <th data-key="climb_max_tomorrow">Max. Kletterbarkeit morgen</th>',
  '          <th data-key="climb_max_time_local">Uhrzeit</th>',
  '          <th>Details</th>',
  '          <th>Topo</th>',
  '        </tr>',
  '      </thead>',
  '      <tbody></tbody>',
  '    </table>',
  '  </div>',
  '',
  '  <div id="modal">',
  '    <div class="inner">',
  '      <div class="bar">',
  '        <div id="modalTitle">Diagramm</div>',
  '        <div style="display:flex; gap:10px; align-items:center;">',
  '          <a id="openNewTab" href="#" target="_blank" rel="noopener">In neuem Tab</a>',
  '          <button id="closeModal">Schliessen</button>',
  '        </div>',
  '      </div>',
  '      <img id="modalImg" src="" alt="Diagramm"/>',
  '    </div>',
  '  </div>',
  '',
  paste0('  <script id="ICEFALL_DATA_B64" type="text/plain">', embedded_b64, '</script>'),
  '',
  '  <script>',
  '  (function(){',
  '    const status = document.getElementById("status");',
  '    const q = document.getElementById("q");',
  '    const tbody = document.querySelector("#tbl tbody");',
  '    const ths = Array.from(document.querySelectorAll("th[data-key]"));',
  '',
  '    const radiusInput = document.getElementById("radiusKm");',
  '    const useGeoBtn = document.getElementById("useGeo");',
  '    const geoStatus = document.getElementById("geoStatus");',
  '    const placeInput = document.getElementById("place");',
  '    const geocodeBtn = document.getElementById("geocodeBtn");',
  '    const placeList = document.getElementById("placeSuggestions");',
  '    const centerLat = document.getElementById("centerLat");',
  '    const centerLon = document.getElementById("centerLon");',
  '    const setCustomBtn = document.getElementById("setCustom");',
  '',
  '    // Grade sliders (min/max)',
  '    const aMin = document.getElementById("aMin");',
  '    const aMax = document.getElementById("aMax");',
  '    const mMin = document.getElementById("mMin");',
  '    const mMax = document.getElementById("mMax");',
  '    const wiMin = document.getElementById("wiMin");',
  '    const wiMax = document.getElementById("wiMax");',
  '    const rMin = document.getElementById("rMin");',
  '    const rMax = document.getElementById("rMax");',
  '    const sunMin = document.getElementById("sunMin");',
  '    const sunMax = document.getElementById("sunMax");',
  '    const aRangeTxt = document.getElementById("aRangeTxt");',
  '    const mRangeTxt = document.getElementById("mRangeTxt");',
  '    const wiRangeTxt = document.getElementById("wiRangeTxt");',
  '    const rRangeTxt = document.getElementById("rRangeTxt");',
  '    const sunRangeTxt = document.getElementById("sunRangeTxt");',
  '',
  '    const modal = document.getElementById("modal");',
  '    const modalImg = document.getElementById("modalImg");',
  '    const modalTitle = document.getElementById("modalTitle");',
  '    const closeModal = document.getElementById("closeModal");',
  '    const openNewTab = document.getElementById("openNewTab");',
  '',
  '    let rows = [];',
  '    let sortKey = "climb_max_tomorrow";',
  '    let sortAsc = false;',
  '',
  '    let center = null;',
  '    let centerLabel = "";',
  '    let radiusKm = NaN;',
  '',
  '    let fAmin = NaN, fAmax = NaN;',
  '    let fMmin = NaN, fMmax = NaN;',
  '    let fWImin = NaN, fWImax = NaN;',
  '    let fRmin = NaN, fRmax = NaN;',
  '    let fSunMin = NaN, fSunMax = NaN;',
  '',
  '    const RANGE = {',
  '      A:  { min: 0.75, max: 4.25 },',
  '      M:  { min: 0.75, max: 13.25 },',
  '      WI: { min: 0.75, max: 7.25 },',
  '      R:  { min: 0.75, max: 12.25 },',
  '      SUN:{ min: 0.00, max: 12.00 }',
  '    };',
  '',
  '    function num(x){',
  '      if (x === null || x === undefined) return NaN;',
  '      const n = Number(x);',
  '      return isFinite(n) ? n : NaN;',
  '    }',
  '    function str(x){',
  '      if (x === null || x === undefined) return "";',
  '      return String(x);',
  '    }',
  '',
  '    function fmtGrade(v){',
  '      const x = num(v);',
  '      if (!isFinite(x)) return "";',
  '      const base = Math.round(x);',
  '      const diff = x - base;',
  '      if (diff > 0.10) return String(base) + "+";',
  '      if (diff < -0.10) return String(base) + "-";',
  '      return String(base);',
  '    }',
  '',
  '    function clampMinMax(minEl, maxEl){',
  '      if (!minEl || !maxEl) return [NaN, NaN];',
  '      let a = Number(minEl.value);',
  '      let b = Number(maxEl.value);',
  '      if (!isFinite(a) || !isFinite(b)) return [NaN, NaN];',
  '      if (a > b) { const t = a; a = b; b = t; minEl.value = a; maxEl.value = b; }',
  '      return [a, b];',
  '    }',
  '',
  '    function updateRangeLabels(){',
  '      const [a1,a2] = clampMinMax(aMin, aMax);',
  '      const [m1,m2] = clampMinMax(mMin, mMax);',
  '      const [w1,w2] = clampMinMax(wiMin, wiMax);',
  '      const [r1,r2] = clampMinMax(rMin, rMax);',
  '      const [s1,s2] = clampMinMax(sunMin, sunMax);',
  '      if (aRangeTxt && isFinite(a1) && isFinite(a2)) aRangeTxt.textContent = `A${fmtGrade(a1)} – A${fmtGrade(a2)}`;',
  '      if (mRangeTxt && isFinite(m1) && isFinite(m2)) mRangeTxt.textContent = `M${fmtGrade(m1)} – M${fmtGrade(m2)}`;',
  '      if (wiRangeTxt && isFinite(w1) && isFinite(w2)) wiRangeTxt.textContent = `WI${fmtGrade(w1)} – WI${fmtGrade(w2)}`;',
  '      if (rRangeTxt && isFinite(r1) && isFinite(r2)) rRangeTxt.textContent = `${fmtGrade(r1)} – ${fmtGrade(r2)}`;',
  '      if (sunRangeTxt && isFinite(s1) && isFinite(s2)) sunRangeTxt.textContent = `${s1.toFixed(1)} – ${s2.toFixed(1)} h`;',
  '    }',
  '',
  '    function parseDifficulty(d){',
  '      const s0 = str(d).toUpperCase();',
  '      const s = s0.replace(/SCHWIERIGKEIT|DIFFICULTY|GRADE|GRAD/g, " ");',
  '      const out = { a: NaN, m: NaN, wi: NaN, r: NaN };',
  '      let m = null;',
  '',
  '      function signed(base, sign){',
  '        const n = Number(base);',
  '        if (!isFinite(n)) return NaN;',
  '        if (sign === "+") return n + 0.25;',
  '        if (sign === "-") return n - 0.25;',
  '        return n;',
  '      }',
  '',
  '      m = s.match(/(?:^|[^A-Z])A\\s*(\\d{1,2})\\s*([+\\-])?/);',
  '      if (m) out.a = signed(m[1], m[2]);',
  '',
  '      m = s.match(/(?:^|[^A-Z])M\\s*(\\d{1,2})\\s*([+\\-])?/);',
  '      if (m) out.m = signed(m[1], m[2]);',
  '',
  '      m = s.match(/(?:^|[^A-Z])WI\\s*(\\d{1,2})\\s*([+\\-])?/);',
  '      if (m) out.wi = signed(m[1], m[2]);',
  '',
  '      // Standalone rock grades (1..12) with optional +/-; take max if multiple',
  '      const re = /(?:^|[^A-Z0-9])(1[0-2]|[1-9])\\s*([+\\-])?(?=\\b|[^0-9])/g;',
  '      let best = NaN;',
  '      while ((m = re.exec(s)) !== null) {',
  '        const v = signed(m[1], m[2]);',
  '        if (isFinite(v)) best = isFinite(best) ? Math.max(best, v) : v;',
  '      }',
  '      out.r = best;',
  '',
  '      return out;',
  '    }',
  '',
  '    function haversineKm(lat1, lon1, lat2, lon2){',
  '      const R = 6371;',
  '      const toRad = (d) => (d * Math.PI / 180);',
  '      const dLat = toRad(lat2 - lat1);',
  '      const dLon = toRad(lon2 - lon1);',
  '      const a = Math.sin(dLat/2) * Math.sin(dLat/2) +',
  '                Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *',
  '                Math.sin(dLon/2) * Math.sin(dLon/2);',
  '      const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));',
  '      return R * c;',
  '    }',
  '',
  '    function computeRowDistance(r){',
  '      if (!center) { r._dist_km = NaN; return NaN; }',
  '      const lat = num(r.latitude);',
  '      const lon = num(r.longitude);',
  '      if (!isFinite(lat) || !isFinite(lon)) { r._dist_km = NaN; return NaN; }',
  '      const d = haversineKm(center.lat, center.lon, lat, lon);',
  '      r._dist_km = d;',
  '      return d;',
  '    }',
  '',
  '    function matches(r, query){',
  '      // Radius filter (optional)',
  '      if (center && isFinite(radiusKm)) {',
  '        const d = num(r._dist_km);',
  '        if (!isFinite(d) || d > radiusKm) return false;',
  '      }',
  '',
  '      // Grade filters (optional)',
  '      if (isFinite(fAmin) || isFinite(fAmax)) {',
  '        const v = num(r._grade_a);',
  '        if (!isFinite(v)) return false;',
  '        if (isFinite(fAmin) && v < fAmin) return false;',
  '        if (isFinite(fAmax) && v > fAmax) return false;',
  '      }',
  '      if (isFinite(fMmin) || isFinite(fMmax)) {',
  '        const v = num(r._grade_m);',
  '        if (!isFinite(v)) return false;',
  '        if (isFinite(fMmin) && v < fMmin) return false;',
  '        if (isFinite(fMmax) && v > fMmax) return false;',
  '      }',
  '      if (isFinite(fWImin) || isFinite(fWImax)) {',
  '        const v = num(r._grade_wi);',
  '        if (!isFinite(v)) return false;',
  '        if (isFinite(fWImin) && v < fWImin) return false;',
  '        if (isFinite(fWImax) && v > fWImax) return false;',
  '      }',
  '      if (isFinite(fRmin) || isFinite(fRmax)) {',
  '        const v = num(r._grade_r);',
  '        if (!isFinite(v)) return false;',
  '        if (isFinite(fRmin) && v < fRmin) return false;',
  '        if (isFinite(fRmax) && v > fRmax) return false;',
  '      }',
  '      if (isFinite(fSunMin) || isFinite(fSunMax)) {',
  '        const v = num(r.sun_hours_tomorrow_h);',
  '        if (!isFinite(v)) return false;',
  '        if (isFinite(fSunMin) && v < fSunMin) return false;',
  '        if (isFinite(fSunMax) && v > fSunMax) return false;',
  '      }',
  '',
  '      if(!query) return true;',
  '      const t = query.toLowerCase();',
  '      const blob = [',
  '        r.name, r.difficulty,',
  '        (isFinite(num(r._grade_a)) ? ("A" + fmtGrade(r._grade_a)) : ""),',
  '        (isFinite(num(r._grade_m)) ? ("M" + fmtGrade(r._grade_m)) : ""),',
  '        (isFinite(num(r._grade_wi)) ? ("WI" + fmtGrade(r._grade_wi)) : ""),',
  '        (isFinite(num(r._grade_r)) ? fmtGrade(r._grade_r) : ""),',
  '        r.aspect, r.station_id, r.source, r.approach, r.descent',
  '      ].map(str).join(" | ").toLowerCase();',
  '      return blob.includes(t);',
  '    }',
  '',
  '    function cmp(a,b){',
  '      const va = a[sortKey];',
  '      const vb = b[sortKey];',
  '      const na = num(va), nb = num(vb);',
  '      const aMiss = !isFinite(na);',
  '      const bMiss = !isFinite(nb);',
  '      if (aMiss && !bMiss) return 1;',
  '      if (!aMiss && bMiss) return -1;',
  '      if (!aMiss && !bMiss) return sortAsc ? (na-nb) : (nb-na);',
  '      const sa = str(va).toLowerCase();',
  '      const sb = str(vb).toLowerCase();',
  '      if (sa < sb) return sortAsc ? -1 : 1;',
  '      if (sa > sb) return sortAsc ? 1 : -1;',
  '      return 0;',
  '    }',
  '',
  '    function openFullscreen(plotUrl, title){',
  '      modalImg.src = plotUrl;',
  '      modalTitle.textContent = title || "Diagramm";',
  '      openNewTab.href = plotUrl;',
  '      modal.style.display = "block";',
  '    }',
  '    function closeFullscreen(){',
  '      modal.style.display = "none";',
  '      modalImg.src = "";',
  '    }',
  '',
  '    if (closeModal) closeModal.addEventListener("click", closeFullscreen);',
  '    if (modal) modal.addEventListener("click", function(e){ if (e.target === modal) closeFullscreen(); });',
  '    document.addEventListener("keydown", function(e){ if (e.key === "Escape") closeFullscreen(); });',
  '',
  '    function applySliderFilters(){',
  '      // radius',
  '      radiusKm = radiusInput ? Number(radiusInput.value) : NaN;',
  '      if (!isFinite(radiusKm) || radiusKm <= 0) radiusKm = NaN;',
  '',
  '      // grade ranges',
  '      let a = clampMinMax(aMin, aMax);',
  '      let m = clampMinMax(mMin, mMax);',
  '      let w = clampMinMax(wiMin, wiMax);',
  '      let r = clampMinMax(rMin, rMax);',
  '      let s = clampMinMax(sunMin, sunMax);',
  '',
  '      fAmin = (isFinite(a[0]) && a[0] > RANGE.A.min + 1e-9) ? a[0] : NaN;',
  '      fAmax = (isFinite(a[1]) && a[1] < RANGE.A.max - 1e-9) ? a[1] : NaN;',
  '      fMmin = (isFinite(m[0]) && m[0] > RANGE.M.min + 1e-9) ? m[0] : NaN;',
  '      fMmax = (isFinite(m[1]) && m[1] < RANGE.M.max - 1e-9) ? m[1] : NaN;',
  '      fWImin = (isFinite(w[0]) && w[0] > RANGE.WI.min + 1e-9) ? w[0] : NaN;',
  '      fWImax = (isFinite(w[1]) && w[1] < RANGE.WI.max - 1e-9) ? w[1] : NaN;',
  '      fRmin = (isFinite(r[0]) && r[0] > RANGE.R.min + 1e-9) ? r[0] : NaN;',
  '      fRmax = (isFinite(r[1]) && r[1] < RANGE.R.max - 1e-9) ? r[1] : NaN;',
  '      fSunMin = (isFinite(s[0]) && s[0] > RANGE.SUN.min + 1e-9) ? s[0] : NaN;',
  '      fSunMax = (isFinite(s[1]) && s[1] < RANGE.SUN.max - 1e-9) ? s[1] : NaN;',
  '',
  '      updateRangeLabels();',
  '    }',
  '',
  '    function render(){',
  '      applySliderFilters();',
  '      for (const r of rows) computeRowDistance(r);',
  '      const query = q.value.trim();',
  '      const view = rows.filter(r => matches(r, query)).sort(cmp);',
  '',
  '      tbody.innerHTML = "";',
  '      for(const r of view){',
  '        const tr = document.createElement("tr");',
  '        const topoLink = r.topo_url ? `<a href="${r.topo_url}" target="_blank" rel="noopener">Topo</a>` : `<span class="muted">&mdash;</span>`;',
  '        const uidPad = String(r.uid).padStart(3,"0");',
  '   const detailsUrl = `icefalls/uid_${uidPad}.html`;',
  '   const detailsBtn = `<a class="btn" href="${detailsUrl}" target="_blank" rel="noopener">Öffnen</a>`;',
  '',
  '        const aTxt  = isFinite(num(r._grade_a))  ? ("A" + fmtGrade(r._grade_a))  : "<span class=muted>&mdash;</span>";',
  '        const mTxt  = isFinite(num(r._grade_m))  ? ("M" + fmtGrade(r._grade_m))  : "<span class=muted>&mdash;</span>";',
  '        const wiTxt = isFinite(num(r._grade_wi)) ? ("WI" + fmtGrade(r._grade_wi)) : "<span class=muted>&mdash;</span>";',
  '        const rTxt  = isFinite(num(r._grade_r))  ? fmtGrade(r._grade_r)  : "<span class=muted>&mdash;</span>";',
  '',
  '        tr.innerHTML = `',
  '          <td>',
  '            <div><b>${str(r.name) || ("UID " + r.uid)}</b></div>',
  '            <div class="muted">${r.station_id ? ("Station: " + str(r.station_id) + (r.source ? (" (" + str(r.source) + ")") : "")) : ""}</div>',
  '          </td>',
  '          <td>${str(r.difficulty) || "<span class=muted>&mdash;</span>"}</td>',
  '          <td>${aTxt}</td>',
  '          <td>${mTxt}</td>',
  '          <td>${wiTxt}</td>',
  '          <td>${rTxt}</td>',
  '          <td>${isFinite(num(r.elev_m)) ? Math.round(num(r.elev_m)) : "<span class=muted>&mdash;</span>"}</td>',
  '          <td>${isFinite(num(r._dist_km)) ? (num(r._dist_km).toFixed(1) + " km") : "<span class=muted>&mdash;</span>"}</td>',
  '          <td>${str(r.sun_tomorrow_range_txt) || "<span class=muted>&mdash;</span>"}</td>',
  '          <td>${isFinite(num(r.sun_hours_tomorrow_h)) ? (num(r.sun_hours_tomorrow_h).toFixed(1) + " h") : "<span class=muted>&mdash;</span>"}</td>',
  '          <td>${r.thickness_tomorrow_07_txt || "<span class=muted>&mdash;</span>"}</td>',
  '          <td>${r.climb_max_tomorrow_txt || "<span class=muted>&mdash;</span>"}</td>',
  '          <td>${str(r.climb_max_time_local) || "<span class=muted>&mdash;</span>"}</td>',
' <td>${detailsBtn}</td>',
  '          <td>${topoLink}</td>',
  '        `;',
  '        tbody.appendChild(tr);',
  '      }',
  '',
  '      Array.from(document.querySelectorAll("button[data-plot]"))',
  '        .forEach(btn => btn.addEventListener("click", () => openFullscreen(btn.getAttribute("data-plot"), btn.getAttribute("data-title"))));',
  '',
  '      const dirTxt = sortAsc ? "ASC" : "DESC";',
  '      const radiusTxt = (center && isFinite(radiusKm)) ? (` | Umkreis: ${radiusKm} km um ${centerLabel || "Zentrum"}`) : "";',
  '      const centerOnlyTxt = (center && !isFinite(radiusKm)) ? (` | Zentrum: ${centerLabel || "Zentrum"} (Radius aus)`) : "";',
  '      status.textContent = `Eintraege: ${view.length} / ${rows.length}${radiusTxt || centerOnlyTxt} | Sort: ${sortKey} ${dirTxt}`;',
  '    }',
  '',
  '    ths.forEach(th => {',
  '      th.addEventListener("click", () => {',
  '        const key = th.getAttribute("data-key");',
  '        if (key === sortKey) sortAsc = !sortAsc; else { sortKey = key; sortAsc = true; }',
  '        render();',
  '      });',
  '    });',
  '',
  '    q.addEventListener("input", render);',
  '    if (radiusInput) radiusInput.addEventListener("input", render);',
  '    if (aMin) aMin.addEventListener("input", render);',
  '    if (aMax) aMax.addEventListener("input", render);',
  '    if (mMin) mMin.addEventListener("input", render);',
  '    if (mMax) mMax.addEventListener("input", render);',
  '    if (wiMin) wiMin.addEventListener("input", render);',
  '    if (wiMax) wiMax.addEventListener("input", render);',
  '    if (rMin) rMin.addEventListener("input", render);',
  '    if (rMax) rMax.addEventListener("input", render);',
  '    if (sunMin) sunMin.addEventListener("input", render);',
  '    if (sunMax) sunMax.addEventListener("input", render);',
  '',
  '    // Custom coordinate center',
  '    if (setCustomBtn) setCustomBtn.addEventListener("click", () => {',
  '      const lat = centerLat ? Number(centerLat.value) : NaN;',
  '      const lon = centerLon ? Number(centerLon.value) : NaN;',
  '      if (!isFinite(lat) || !isFinite(lon)) { if (geoStatus) geoStatus.textContent = "Koordinaten ungueltig"; return; }',
  '      center = { lat, lon };',
  '      centerLabel = lat.toFixed(5) + "," + lon.toFixed(5);',
  '      if (geoStatus) geoStatus.textContent = "Zentrum gesetzt: " + centerLabel;',
  '      render();',
  '    });',
  '',
  '    function geocodePlace(query){',
  '      const q = (query || "").trim();',
  '      if (!q) return Promise.reject(new Error("Kein Ort eingegeben"));',
  '      const url = "https://nominatim.openstreetmap.org/search?format=json&limit=1&q=" + encodeURIComponent(q) + "&countrycodes=at,de,it,ch";',
  '      return fetch(url, { cache: "no-store", headers: { "Accept": "application/json" } })',
  '        .then(r => { if (!r.ok) throw new Error("Geocoding HTTP " + r.status); return r.json(); })',
  '        .then(arr => {',
  '          if (!Array.isArray(arr) || arr.length === 0) throw new Error("Ort nicht gefunden");',
  '          const hit = arr[0];',
  '          const lat = Number(hit.lat);',
  '          const lon = Number(hit.lon);',
  '          const label = hit.display_name ? String(hit.display_name).split(",")[0] : q;',
  '          if (!isFinite(lat) || !isFinite(lon)) throw new Error("Geocoding ohne Koordinaten");',
  '          return { lat, lon, label };',
  '        });',
  '    }',
  '',
  '    // Typeahead suggestions (Nominatim). Needs Internet; keep usage light (debounce + min length).',
  '    let sugTimer = null;',
  '    let lastSug = [];',
  '    function clearSuggestions(){',
  '      lastSug = [];',
  '      if (!placeList) return;',
  '      placeList.innerHTML = "";',
  '    }',
  '    function setSuggestions(items){',
  '      lastSug = items || [];',
  '      if (!placeList) return;',
  '      placeList.innerHTML = "";',
  '      for (const it of lastSug){',
  '        const opt = document.createElement("option");',
  '        opt.value = it.label;',
  '        placeList.appendChild(opt);',
  '      }',
  '    }',
  '    function fetchSuggestions(q){',
  '      const qq = (q || "").trim();',
  '      if (qq.length < 3) { clearSuggestions(); return; }',
  '      const url = "https://nominatim.openstreetmap.org/search?format=json&limit=6&q=" + encodeURIComponent(qq) + "&countrycodes=at,de,it,ch";',
  '      fetch(url, { cache: "no-store", headers: { "Accept": "application/json" } })',
  '        .then(r => { if (!r.ok) throw new Error("Suggest HTTP " + r.status); return r.json(); })',
  '        .then(arr => {',
  '          if (!Array.isArray(arr) || arr.length === 0) { clearSuggestions(); return; }',
  '          const items = arr.map(h => {',
  '            const lat = Number(h.lat);',
  '            const lon = Number(h.lon);',
  '            const label = h.display_name ? String(h.display_name).split(",").slice(0,3).join(", ") : qq;',
  '            return { label, lat, lon };',
  '          }).filter(x => isFinite(x.lat) && isFinite(x.lon));',
  '          setSuggestions(items);',
  '        })',
  '        .catch(_ => { /* silent */ });',
  '    }',
  '    if (placeInput) placeInput.addEventListener("input", () => {',
  '      if (sugTimer) clearTimeout(sugTimer);',
  '      const v = placeInput.value;',
  '      sugTimer = setTimeout(() => fetchSuggestions(v), 350);',
  '    });',
  '    if (placeInput) placeInput.addEventListener("keydown", (e) => { if (e.key === "Enter" && geocodeBtn) geocodeBtn.click(); });',
  '    if (geocodeBtn) geocodeBtn.addEventListener("click", () => {',
  '      if (!placeInput) return;',
  '      const v = (placeInput.value || "").trim();',
  '      if (!v) { if (geoStatus) geoStatus.textContent = "Kein Ort eingegeben"; return; }',
  '      const hit = lastSug.find(s => String(s.label).toLowerCase() === v.toLowerCase());',
  '      if (hit && isFinite(hit.lat) && isFinite(hit.lon)) {',
  '        center = { lat: hit.lat, lon: hit.lon };',
  '        centerLabel = hit.label;',
  '        if (centerLat) centerLat.value = hit.lat.toFixed(6);',
  '        if (centerLon) centerLon.value = hit.lon.toFixed(6);',
  '        if (geoStatus) geoStatus.textContent = "Zentrum gesetzt: " + centerLabel;',
  '        render();',
  '        return;',
  '      }',
  '      if (geoStatus) geoStatus.textContent = "Suche Ort ...";',
  '      geocodePlace(v)',
  '        .then(res => {',
  '          center = { lat: res.lat, lon: res.lon };',
  '          centerLabel = res.label;',
  '          if (centerLat) centerLat.value = res.lat.toFixed(6);',
  '          if (centerLon) centerLon.value = res.lon.toFixed(6);',
  '          if (geoStatus) geoStatus.textContent = "Zentrum gesetzt: " + centerLabel;',
  '          render();',
  '        })',
  '        .catch(err => { if (geoStatus) geoStatus.textContent = "Ortsuche fehlgeschlagen: " + (err && err.message ? err.message : err); });',
  '    });',
  '',
  '    if (useGeoBtn) useGeoBtn.addEventListener("click", () => {',
  '      if (!navigator.geolocation) { if (geoStatus) geoStatus.textContent = "Geolocation nicht verfuegbar"; return; }',
  '      if (window.isSecureContext !== true) { if (geoStatus) geoStatus.textContent = "GPS benoetigt https oder localhost (file:// ist meist blockiert)."; return; }',
  '      if (geoStatus) geoStatus.textContent = "Hole Standort ...";',
  '      navigator.geolocation.getCurrentPosition(',
  '        (pos) => {',
  '          const lat = pos.coords.latitude;',
  '          const lon = pos.coords.longitude;',
  '          if (centerLat) centerLat.value = lat.toFixed(6);',
  '          if (centerLon) centerLon.value = lon.toFixed(6);',
  '          center = { lat: lat, lon: lon };',
  '          centerLabel = "Standort";',
  '          if (geoStatus) geoStatus.textContent = "Zentrum: Standort";',
  '          render();',
  '        },',
  '        (err) => { if (geoStatus) geoStatus.textContent = "Standort fehlgeschlagen: " + (err && err.message ? err.message : err); },',
  '        { enableHighAccuracy: false, timeout: 10000, maximumAge: 600000 }',
  '      );',
  '    });',
  '',
  '    function enrichRows(){',
  '      for (const r of rows){',
  '        const g = parseDifficulty(r.difficulty);',
  '        r._grade_a = g.a;',
  '        r._grade_m = g.m;',
  '        r._grade_wi = g.wi;',
  '        r._grade_r = g.r;',
  '      }',
  '    }',
  '',
  '    function initSunSliderFromData(){',
  '      // set SUN max to data-driven max (rounded up), keep within [2, 24]',
  '      let mx = 0;',
  '      for (const r of rows){',
  '        const v = num(r.sun_hours_tomorrow_h);',
  '        if (isFinite(v)) mx = Math.max(mx, v);',
  '      }',
  '      if (!isFinite(mx) || mx <= 0) mx = RANGE.SUN.max;',
  '      const maxNice = Math.min(24, Math.max(2, Math.ceil(mx * 4) / 4));',
  '      RANGE.SUN.max = maxNice;',
  '      if (sunMin) { sunMin.max = String(maxNice); if (Number(sunMin.value) > maxNice) sunMin.value = String(maxNice); }',
  '      if (sunMax) { sunMax.max = String(maxNice); sunMax.value = String(maxNice); }',
  '      updateRangeLabels();',
  '    }',
  '',
  '    try {',
  '      const b64el = document.getElementById("ICEFALL_DATA_B64");',
  '      const b64 = (b64el && b64el.textContent) ? b64el.textContent.trim() : "";',
  '      if (b64.length > 10) {',
  '        const bin = atob(b64);',
  '        const bytes = Uint8Array.from(bin, c => c.charCodeAt(0));',
  '        const jsonText = new TextDecoder("utf-8").decode(bytes);',
  '        rows = JSON.parse(jsonText);',
  '        if (!Array.isArray(rows)) rows = [];',
  '        enrichRows();',
  '        initSunSliderFromData();',
  '        status.textContent = `Daten geladen (embedded): ${rows.length} Eintraege`;',
  '        render();',
  '        return;',
  '      }',
  '    } catch(e) { console.error("Embedded Base64 parse failed", e); }',
  '',
  '    status.textContent = "Kein embedded JSON gefunden. Versuche fetch() ...";',
  '    const candidates = ["icefalls_table.json", "./icefalls_table.json", "../icefalls_table.json", "site/icefalls_table.json", "./site/icefalls_table.json", "../site/icefalls_table.json"];',
  '    function fetchJsonFirstOk(urls){',
  '      return urls.reduce((p, u) => p.catch(() => fetch(u, {cache: "no-store"}).then(r => {',
  '        if (!r.ok) throw new Error(u + " -> HTTP " + r.status);',
  '        return r.json().then(data => ({ data, url: u }));',
  '      })), Promise.reject(new Error("no candidates tried")));',
  '    }',
  '    fetchJsonFirstOk(candidates)',
  '      .then(res => {',
  '        rows = (res && res.data) ? res.data : [];',
  '        if (!Array.isArray(rows)) rows = [];',
  '        enrichRows();',
  '        initSunSliderFromData();',
  '        status.textContent = `Daten geladen: ${rows.length} Eintraege (Quelle: ${res.url})`;',
  '        render();',
  '      })',
  '      .catch(err => { status.textContent = "Fehler beim Laden: " + err; console.error(err); });',
  '  })();',
  '  </script>',
  '</body>',
  '</html>'
)

html <- paste(html_lines, collapse = "\n")

writeLines(enc2utf8(html), OUT_HTML, useBytes = TRUE)
message("✅ Wrote HTML: ", OUT_HTML)

# ----------------------------
# 8) Write detail pages per UID (site/icefalls/uid_###.html)
# ----------------------------

DETAIL_DIR <- file.path(OUT_DIR, "icefalls")
dir.create(DETAIL_DIR, showWarnings = FALSE, recursive = TRUE)

# Cloudflare Worker + R2 Public base
API_BASE  <- "https://icefalls-api.carlos-wydra.workers.dev"
R2_PUBLIC <- "https://pub-1b553d6f009540c0881b434b7791c67c.r2.dev"

# Helper: safe HTML text (very small; JS does most escaping)
esc_html <- function(x) {
  x <- ifelse(is.na(x) | x %in% c("", "NA", "NaN", "NULL"), "", as.character(x))
  x <- gsub("&", "&amp;", x, fixed = TRUE)
  x <- gsub("<", "&lt;",  x, fixed = TRUE)
  x <- gsub(">", "&gt;",  x, fixed = TRUE)
  x <- gsub("\"", "&quot;", x, fixed = TRUE)
  x
}

# Iterate over all rows (one detail page per uid)
for (i in seq_len(nrow(out))) {
  
  r <- out[i, , drop = FALSE]
  
  uid <- as.integer(r$uid[[1]])
  if (!is.finite(uid)) next
  
  uid_pad <- sprintf("%03d", uid)
  
  nm  <- esc_html(r$name[[1]])
  diff <- esc_html(r$difficulty[[1]])
  elev <- r$elev_m[[1]]
  elev_txt <- if (is.finite(elev)) paste0(round(elev), " m") else "&mdash;"
  
  topo_url <- as.character(r$topo_url[[1]])
  topo_url <- ifelse(is.na(topo_url) | topo_url == "", "", topo_url)
  
  lat <- r$latitude[[1]]
  lon <- r$longitude[[1]]
  lat_txt <- if (is.finite(lat)) sprintf("%.6f", lat) else ""
  lon_txt <- if (is.finite(lon)) sprintf("%.6f", lon) else ""
  
  # plot png relative to site root
  plot_rel <- sprintf("plots/uid_%03d.png", uid)
  
  out_file <- file.path(DETAIL_DIR, sprintf("uid_%s.html", uid_pad))
  
  detail_html <- c(
    "<!doctype html>",
    "<html lang='de'>",
    "<head>",
    "  <meta charset='utf-8'/>",
    "  <meta name='viewport' content='width=device-width, initial-scale=1'/>",
    paste0("  <title>", nm, " (UID ", uid_pad, ")</title>"),
    
    # Leaflet (small map)
    "  <link rel='stylesheet' href='https://unpkg.com/leaflet@1.9.4/dist/leaflet.css'/>",
    "  <script src='https://unpkg.com/leaflet@1.9.4/dist/leaflet.js'></script>",
    
    "  <style>",
    "    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:0;padding:0;background:#fff;}",
    "    header{padding:10px 14px;border-bottom:1px solid #ddd;display:flex;gap:10px;align-items:center;flex-wrap:wrap;}",
    "    header a{text-decoration:none;padding:6px 10px;border:1px solid #ddd;border-radius:10px;color:#111;background:#fff;}",
    "    header a:hover{background:#f4f4f4;}",
    "    .wrap{padding:12px 14px;max-width:1400px;margin:0 auto;}",
    
    "    .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;}",
    "    @media(max-width:900px){.grid2{grid-template-columns:1fr;}}",
    
    "    .card{border:1px solid #eee;border-radius:16px;padding:12px 14px;background:#fff;}",
    "    .card h2{margin:0 0 8px 0;font-size:16px;}",
    "    .kv{display:grid;grid-template-columns:170px 1fr;gap:6px 10px;font-size:14px;}",
    "    .k{color:#666;}",
    "    .muted{color:#666;font-size:12px;}",
    
    "    .btn{display:inline-flex;gap:6px;align-items:center;padding:8px 12px;border:1px solid #ddd;border-radius:12px;background:#fff;cursor:pointer;text-decoration:none;color:#111;}",
    "    .btn:hover{background:#f4f4f4;}",
    
    "    .gallery{display:flex;flex-wrap:wrap;gap:10px;margin-top:6px;}",
    "    .ph{width:220px;border:1px solid #eee;border-radius:14px;overflow:hidden;background:#fff;}",
    "    .ph img{width:100%;height:160px;object-fit:cover;display:block;}",
    "    .cap{padding:8px 10px;font-size:12px;}",
    
    "    input[type='file'], input[type='date'], select, textarea{padding:10px 12px;border:1px solid #ccc;border-radius:12px;font-size:14px;}",
    "    textarea{resize:vertical;}",
    
    # diagram full width
    "    .plotWrap{margin-top:12px;}",
    "    .plotImg{width:100%;height:auto;display:block;border:1px solid #eee;border-radius:16px;}",
    
    "    #map{height:240px;border-radius:16px;border:1px solid #eee;}",
    "  </style>",
    "</head>",
    "<body>",
    
    "<header>",
    "  <a href='../list.html'>&larr; &Uuml;bersicht</a>",
    "  <a href='../index.html'>Karte</a>",
    paste0("  <div style='font-weight:700;'>", nm, " <span class='muted'>(UID ", uid_pad, ")</span></div>"),
    "</header>",
    
    "<div class='wrap'>",
    
    # top row: Basic Infos + Bilder (empty state initially)
    "  <div class='grid2'>",
    "    <div class='card'>",
    "      <h2>Basic Infos</h2>",
    "      <div class='kv'>",
    paste0("        <div class='k'>Schwierigkeit</div><div><b>", ifelse(nchar(diff)>0, diff, "&mdash;"), "</b></div>"),
    paste0("        <div class='k'>H&ouml;he &uuml;. NN</div><div><b>", elev_txt, "</b></div>"),
    paste0("        <div class='k'>Topo</div><div>", ifelse(nchar(topo_url)>0, paste0("<a href='", topo_url, "' target='_blank' rel='noopener'>Topo</a>"), "<span class='muted'>&mdash;</span>"), "</div>"),
    "      </div>",
    "    </div>",
    
    "    <div class='card'>",
    "      <h2>Bilder</h2>",
    "      <div class='muted' style='display:flex;justify-content:space-between;gap:10px;align-items:center;'>",
    "        <div id='imgStatus'>Lade...</div>",
    "        <div>Uploads werden &ouml;ffentlich angezeigt.</div>",
    "      </div>",
    "      <div id='gallery' class='gallery' style='margin-top:10px;'></div>",
    "    </div>",
    "  </div>",
    
    # full-width plot card
    "  <div class='card plotWrap'>",
    "    <h2>Diagramm</h2>",
    paste0("    <div style='margin-bottom:8px;'><a class='btn' href='../", plot_rel, "' target='_blank' rel='noopener'>gro&szlig; &ouml;ffnen</a></div>"),
    paste0("    <img class='plotImg' src='../", plot_rel, "' alt='Diagramm UID ", uid_pad, "' onerror=\"this.outerHTML='<div class=\\\"muted\\\">Kein Plot gefunden.</div>';\"/>"),
    "  </div>",
    
    # upload + map row
    "  <div class='grid2' style='margin-top:12px;'>",
    
    # upload card
    "    <div class='card'>",
    "      <h2>Bild hochladen</h2>",
    "      <div class='muted' style='margin-bottom:10px;'>Mit Upload best&auml;tigst du, dass du die Rechte am Bild hast. Bitte keine personenbezogenen Daten.</div>",
    
    "      <div style='display:flex;gap:10px;flex-wrap:wrap;align-items:center;'>",
    "        <div class='muted' style='min-width:110px;'>Kletterbarkeit</div>",
    "        <select id='rating'>",
    "          <option value='3'>gut</option>",
    "          <option value='2'>grenzwertig</option>",
    "          <option value='1'>gar nicht</option>",
    "        </select>",
    "        <div class='muted' style='min-width:50px;'>Datum</div>",
    "        <input id='shotDate' type='date'/>",
    "      </div>",
    
    # comment ONLY here (NOT in Basic Infos)
    "      <div style='margin-top:10px;'>",
    "        <textarea id='comment' rows='3' placeholder='Kommentar (optional): Eiszustand, Bedingungen, Gefahren…' style='width:100%;max-width:100%;'></textarea>",
    "      </div>",
    
    "      <div style='display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:10px;'>",
    "        <input id='file' type='file' accept='image/*'/>",
    "        <button id='btnUpload' class='btn' type='button'>Upload</button>",
    "        <span id='upStatus' class='muted'></span>",
    "      </div>",
    "    </div>",
    
    # map card
    "    <div class='card'>",
    "      <h2>Location</h2>",
    paste0("      <div class='muted' style='margin-bottom:8px;'>Koordinaten: ",
           ifelse(nchar(lat_txt)>0 && nchar(lon_txt)>0, paste0(lat_txt, ", ", lon_txt), "&mdash;"),
           "</div>"),
    "      <div id='map'></div>",
    "      <div id='mapStatus' class='muted' style='margin-top:8px;'></div>",
    "    </div>",
    
    "  </div>",
    
    # script
    "  <script>",
    paste0("  const API_BASE = ", jsonlite::toJSON(API_BASE, auto_unbox = TRUE), ";"),
    paste0("  const R2_PUBLIC = ", jsonlite::toJSON(R2_PUBLIC, auto_unbox = TRUE), ";"),
    paste0("  const UID = ", uid, ";"),
    paste0("  const ICE_NAME = ", jsonlite::toJSON(as.character(r$name[[1]]), auto_unbox = TRUE), ";"),
    paste0("  const ICE_LAT = ", ifelse(is.finite(lat), format(lat, scientific = FALSE), "null"), ";"),
    paste0("  const ICE_LON = ", ifelse(is.finite(lon), format(lon, scientific = FALSE), "null"), ";"),
    
    "  const elGal = document.getElementById('gallery');",
    "  const elImgStatus = document.getElementById('imgStatus');",
    "  const elUpStatus = document.getElementById('upStatus');",
    "  const elFile = document.getElementById('file');",
    "  const elRating = document.getElementById('rating');",
    "  const elDate = document.getElementById('shotDate');",
    "  const elComment = document.getElementById('comment');",
    "  const btn = document.getElementById('btnUpload');",
    
    "  function escHtml(s){",
    "    s = (s===null || s===undefined) ? '' : String(s);",
    "    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\"/g,'&quot;');",
    "  }",
    
    "  function ratingTxt(v){",
    "    v = Number(v);",
    "    if (v === 3) return 'gut';",
    "    if (v === 2) return 'grenzwertig';",
    "    if (v === 1) return 'gar nicht';",
    "    return String(v);",
    "  }",
    
    "  async function loadImages(){",
    "    elImgStatus.textContent = 'Lade...';",
    "    elGal.innerHTML = '';",
    "    try {",
    "      const res = await fetch(`${API_BASE}/api/images?uid=${UID}`, { method:'GET' });",
    "      if (!res.ok) throw new Error('HTTP ' + res.status);",
    "      const arr = await res.json();",
    "      if (!Array.isArray(arr) || arr.length === 0){",
    "        elImgStatus.textContent = 'Noch keine Bilder.';",
    "        return;",
    "      }",
    "      elImgStatus.textContent = `Bilder: ${arr.length}`;",
    "      const html = arr.map(x => {",
    "        const url = x.public_url || '';",
    "        const d = escHtml(x.shot_date || '');",
    "        const rt = ratingTxt(x.rating);",
    "        const c = escHtml(x.comment || '');",
    "        const cHtml = c ? `<div style=\"margin-top:6px;\">${c}</div>` : '';",
    "        if (!url) return '';",
    "        return `",
    "          <div class=\"ph\">",
    "            <a href=\"${url}\" target=\"_blank\" rel=\"noopener\">",
    "              <img src=\"${url}\" alt=\"Bild\" loading=\"lazy\"/>",
    "            </a>",
    "            <div class=\"cap\">",
    "              <b>${d}</b>",
    "              <div class=\"muted\">Kletterbarkeit: ${escHtml(rt)}</div>",
    "              ${cHtml}",
    "            </div>",
    "          </div>`;",
    "      }).join('');",
    "      elGal.innerHTML = html;",
    "    } catch(e){",
    "      elImgStatus.textContent = 'Fehler beim Laden: ' + (e && e.message ? e.message : e);",
    "    }",
    "  }",
    
    "  function initDateDefault(){",
    "    // default = today local",
    "    const now = new Date();",
    "    const yyyy = now.getFullYear();",
    "    const mm = String(now.getMonth()+1).padStart(2,'0');",
    "    const dd = String(now.getDate()).padStart(2,'0');",
    "    if (elDate && !elDate.value) elDate.value = `${yyyy}-${mm}-${dd}`;",
    "  }",
    
    "  async function doUpload(){",
    "    const f = elFile && elFile.files && elFile.files[0] ? elFile.files[0] : null;",
    "    if (!f){ elUpStatus.textContent = 'Bitte Datei ausw\u00e4hlen.'; return; }",
    "    const rating = elRating ? Number(elRating.value) : 3;",
    "    const shotDate = elDate ? String(elDate.value || '') : '';",
    "    if (!shotDate){ elUpStatus.textContent = 'Bitte Datum setzen.'; return; }",
    "    const comment = (elComment && elComment.value) ? elComment.value.trim() : '';",
    
    "    elUpStatus.textContent = 'Upload...';",
    "    const form = new FormData();",
    "    form.append('file', f);",
    "    form.append('uid', String(UID));",
    "    form.append('ice_name', ICE_NAME || '');",
    "    form.append('rating', String(rating));",
    "    form.append('shot_date', shotDate);",
    "    if (comment) form.append('comment', comment);",
    
    "    try {",
    "      const res = await fetch(`${API_BASE}/api/upload`, { method:'POST', body: form });",
    "      const txt = await res.text();",
    "      if (!res.ok) throw new Error(txt || ('HTTP ' + res.status));",
    "      elUpStatus.textContent = 'Upload ok.';",
    "      if (elFile) elFile.value = '';",
    "      if (elComment) elComment.value = '';",
    "      await loadImages();",
    "    } catch(e){",
    "      elUpStatus.textContent = 'Upload fehlgeschlagen: ' + (e && e.message ? e.message : e);",
    "    }",
    "  }",
    
    "  function initMap(){",
    "    const mapStatus = document.getElementById('mapStatus');",
    "    if (ICE_LAT === null || ICE_LON === null || !isFinite(ICE_LAT) || !isFinite(ICE_LON)){",
    "      if (mapStatus) mapStatus.textContent = 'Keine Koordinaten verf\u00fcgbar.';",
    "      return;",
    "    }",
    "    try {",
    "      const m = L.map('map', { scrollWheelZoom: false }).setView([ICE_LAT, ICE_LON], 13);",
    "      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {",
    "        maxZoom: 19,",
    "        attribution: '&copy; OpenStreetMap'",
    "      }).addTo(m);",
    "      L.marker([ICE_LAT, ICE_LON]).addTo(m).bindPopup(escHtml(ICE_NAME || ('UID ' + UID))).openPopup();",
    "      if (mapStatus) mapStatus.textContent = 'Karte: OSM';",
    "    } catch(e){",
    "      if (mapStatus) mapStatus.textContent = 'Kartenfehler: ' + (e && e.message ? e.message : e);",
    "    }",
    "  }",
    
    "  if (btn) btn.addEventListener('click', doUpload);",
    "  initDateDefault();",
    "  initMap();",
    "  loadImages();",
    "  </script>",
    
    "</div>",
    "</body>",
    "</html>"
  )
  
  writeLines(detail_html, out_file, useBytes = TRUE)
}

message("✅ Wrote detail pages: ", DETAIL_DIR, " (", length(unique(out$uid)), " UIDs)")



message("Done. Outputs:")
message(" - ", normalizePath(OUT_JSON, winslash = "/", mustWork = FALSE))
message(" - ", normalizePath(OUT_HTML, winslash = "/", mustWork = FALSE))
