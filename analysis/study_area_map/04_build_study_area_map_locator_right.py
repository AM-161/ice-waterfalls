#!/usr/bin/env python3
"""Build a publication-style study-area map for the icefall thesis.

The map deliberately uses the station-assigned operational working
inventory as its point layer. It does not map the broader raw inventory.
"""

from __future__ import annotations

import glob
import json
import math
import re
import textwrap
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import matplotlib.patches as mpatches
from matplotlib.colors import LinearSegmentedColormap, LightSource, Normalize
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd
from PIL import Image
from pyproj import Transformer
import rasterio
from rasterio.enums import Resampling
from rasterio.transform import from_bounds as transform_from_bounds
from rasterio.windows import from_bounds
from rasterio.warp import reproject
import shapefile


def find_repo_root(script: Path) -> Path:
    for candidate in (script.parent, *script.parents):
        if (
            (candidate / "data" / "AWS" / "icefalls_nearest_station.csv").exists()
            and (candidate / ".github").exists()
        ):
            return candidate
    raise RuntimeError("Could not find repository root from script path.")


SCRIPT = Path(__file__).resolve()
REPO = find_repo_root(SCRIPT)
THESIS_ROOT = REPO.parents[2]
OUT_DIR = THESIS_ROOT / "output" / "study_area_map"
CACHE_DIR = OUT_DIR / "_cache"

ASSIGN_PATH = REPO / "data" / "AWS" / "icefalls_nearest_station.csv"
RAW_PATH = REPO / "data" / "Koordinaten_Wasserfaelle" / "eisklettern_links_entries_diff.csv"
DEM_PATH = REPO / "data" / "DEM" / "eudem_dem_3035_europe.tif"
MODEL_DIR = REPO / "data" / "ModelRuns"
MAP_AUTHOR = "C. Wydra"

WGS84 = "EPSG:4326"
MAP_CRS = "EPSG:3035"
WEB_MERCATOR = "EPSG:3857"
TO_3035 = Transformer.from_crs(WGS84, MAP_CRS, always_xy=True)
TO_WGS84 = Transformer.from_crs(MAP_CRS, WGS84, always_xy=True)
TO_3857 = Transformer.from_crs(WGS84, WEB_MERCATOR, always_xy=True)
FROM_3857 = Transformer.from_crs(WEB_MERCATOR, WGS84, always_xy=True)

NORTH_TYROL_BBOX_LONLAT = (10.1, 46.7, 12.2, 47.7)

NE_SOURCES = {
    "countries": {
        "url": "https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_0_countries.zip",
        "stem": "ne_10m_admin_0_countries",
    },
    "admin1": {
        "url": "https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_1_states_provinces.zip",
        "stem": "ne_10m_admin_1_states_provinces",
    },
}

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OSM_TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
OSM_STANDARD_ZOOM = 11


@dataclass
class MapOutputs:
    png: Path
    pdf: Path
    svg: Path
    doc: Path


def to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", ".", regex=False), errors="coerce")


def read_raw_inventory() -> pd.DataFrame:
    raw = pd.read_csv(
        RAW_PATH,
        sep=";",
        dtype=str,
        encoding="utf-8",
        encoding_errors="replace",
    )
    raw.columns = [c.lower() for c in raw.columns]
    raw["uid"] = pd.to_numeric(raw["uid"], errors="coerce").astype("Int64")
    raw["latitude_num"] = to_num(raw.get("latitude", pd.Series(index=raw.index, dtype=str)))
    raw["longitude_num"] = to_num(raw.get("longitude", pd.Series(index=raw.index, dtype=str)))
    raw["elev_num"] = to_num(raw.get("hoehe_dgm5m", pd.Series(index=raw.index, dtype=str)))
    return raw


def read_assignment() -> pd.DataFrame:
    assign = pd.read_csv(ASSIGN_PATH)
    required = {"uid", "icefall_name", "ice_lat", "ice_lon", "icefall_elev_m"}
    missing = sorted(required - set(assign.columns))
    if missing:
        raise ValueError(f"Missing required assignment columns: {', '.join(missing)}")
    assign = assign.copy()
    assign["uid"] = pd.to_numeric(assign["uid"], errors="coerce").astype("Int64")
    assign["ice_lat"] = pd.to_numeric(assign["ice_lat"], errors="coerce")
    assign["ice_lon"] = pd.to_numeric(assign["ice_lon"], errors="coerce")
    assign["icefall_elev_m"] = pd.to_numeric(assign["icefall_elev_m"], errors="coerce")
    assign = assign.sort_values("uid").reset_index(drop=True)
    x, y = TO_3035.transform(assign["ice_lon"].to_numpy(), assign["ice_lat"].to_numpy())
    assign["x_3035"] = x
    assign["y_3035"] = y
    x_3857, y_3857 = TO_3857.transform(assign["ice_lon"].to_numpy(), assign["ice_lat"].to_numpy())
    assign["x_3857"] = x_3857
    assign["y_3857"] = y_3857
    return assign


def model_uids() -> set[int]:
    out: set[int] = set()
    for path in glob.glob(str(MODEL_DIR / "model_uid*.csv")):
        m = re.search(r"model_uid(\d+)\.csv$", Path(path).name)
        if m:
            out.add(int(m.group(1)))
    return out


def projected_bbox_from_lonlat(lon_min: float, lat_min: float, lon_max: float, lat_max: float) -> tuple[float, float, float, float]:
    lons = np.concatenate(
        [
            np.linspace(lon_min, lon_max, 80),
            np.linspace(lon_min, lon_max, 80),
            np.full(80, lon_min),
            np.full(80, lon_max),
        ]
    )
    lats = np.concatenate(
        [
            np.full(80, lat_min),
            np.full(80, lat_max),
            np.linspace(lat_min, lat_max, 80),
            np.linspace(lat_min, lat_max, 80),
        ]
    )
    xs, ys = TO_3035.transform(lons, lats)
    return min(xs), min(ys), max(xs), max(ys)


def pad_to_aspect(extent: tuple[float, float, float, float], aspect: float, pad_m: float) -> tuple[float, float, float, float]:
    xmin, ymin, xmax, ymax = extent
    xmin -= pad_m
    ymin -= pad_m
    xmax += pad_m
    ymax += pad_m
    width = xmax - xmin
    height = ymax - ymin
    current = width / height
    if current < aspect:
        add = (height * aspect - width) / 2
        xmin -= add
        xmax += add
    else:
        add = (width / aspect - height) / 2
        ymin -= add
        ymax += add
    return xmin, ymin, xmax, ymax


def point_extent(points: pd.DataFrame, aspect: float, pad_m: float) -> tuple[float, float, float, float]:
    extent = (
        float(points["x_3035"].min()),
        float(points["y_3035"].min()),
        float(points["x_3035"].max()),
        float(points["y_3035"].max()),
    )
    return pad_to_aspect(extent, aspect=aspect, pad_m=pad_m)


def point_extent_columns(
    points: pd.DataFrame,
    x_col: str,
    y_col: str,
    aspect: float,
    pad_m: float,
) -> tuple[float, float, float, float]:
    extent = (
        float(points[x_col].min()),
        float(points[y_col].min()),
        float(points[x_col].max()),
        float(points[y_col].max()),
    )
    return pad_to_aspect(extent, aspect=aspect, pad_m=pad_m)


def extent_to_lonlat_bbox(extent: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
    xmin, ymin, xmax, ymax = extent
    xs = np.concatenate(
        [
            np.linspace(xmin, xmax, 80),
            np.linspace(xmin, xmax, 80),
            np.full(80, xmin),
            np.full(80, xmax),
        ]
    )
    ys = np.concatenate(
        [
            np.full(80, ymin),
            np.full(80, ymax),
            np.linspace(ymin, ymax, 80),
            np.linspace(ymin, ymax, 80),
        ]
    )
    lons, lats = TO_WGS84.transform(xs, ys)
    return float(np.nanmin(lons)), float(np.nanmin(lats)), float(np.nanmax(lons)), float(np.nanmax(lats))


def extent_3857_to_lonlat_bbox(extent: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
    xmin, ymin, xmax, ymax = extent
    xs = np.concatenate(
        [
            np.linspace(xmin, xmax, 80),
            np.linspace(xmin, xmax, 80),
            np.full(80, xmin),
            np.full(80, xmax),
        ]
    )
    ys = np.concatenate(
        [
            np.full(80, ymin),
            np.full(80, ymax),
            np.linspace(ymin, ymax, 80),
            np.linspace(ymin, ymax, 80),
        ]
    )
    lons, lats = FROM_3857.transform(xs, ys)
    return float(np.nanmin(lons)), float(np.nanmin(lats)), float(np.nanmax(lons)), float(np.nanmax(lats))


def fetch_natural_earth(kind: str) -> Path:
    meta = NE_SOURCES[kind]
    target_dir = CACHE_DIR / meta["stem"]
    shp_path = target_dir / f"{meta['stem']}.shp"
    if shp_path.exists():
        return shp_path
    target_dir.mkdir(parents=True, exist_ok=True)
    zip_path = CACHE_DIR / f"{meta['stem']}.zip"
    if not zip_path.exists():
        req = urllib.request.Request(
            meta["url"],
            headers={"User-Agent": "icefall-thesis-study-area-map/1.0"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            zip_path.write_bytes(resp.read())
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(target_dir)
    return shp_path


def shape_intersects_bbox(shape_bbox: list[float], bbox: tuple[float, float, float, float]) -> bool:
    sxmin, symin, sxmax, symax = shape_bbox
    xmin, ymin, xmax, ymax = bbox
    return not (sxmax < xmin or sxmin > xmax or symax < ymin or symin > ymax)


def iter_shape_parts(shp_path: Path, bbox_lonlat: tuple[float, float, float, float]):
    reader = shapefile.Reader(str(shp_path), encoding="latin1")
    fields = [field[0] for field in reader.fields[1:]]
    for shape_record in reader.iterShapeRecords():
        shp = shape_record.shape
        if not shape_intersects_bbox(shp.bbox, bbox_lonlat):
            continue
        rec = dict(zip(fields, shape_record.record))
        pts = np.asarray(shp.points, dtype=float)
        if len(pts) == 0:
            continue
        parts = list(shp.parts) + [len(pts)]
        for start, end in zip(parts[:-1], parts[1:]):
            part = pts[start:end]
            if len(part) < 2:
                continue
            xs, ys = TO_3035.transform(part[:, 0], part[:, 1])
            yield rec, np.asarray(xs), np.asarray(ys)


def lonlat_to_osm_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    lat = max(min(lat, 85.05112878), -85.05112878)
    n = 2**zoom
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return max(0, min(n - 1, x)), max(0, min(n - 1, y))


def fetch_osm_tile(z: int, x: int, y: int) -> Path:
    tile_dir = CACHE_DIR / "osm_standard_tiles" / str(z) / str(x)
    tile_dir.mkdir(parents=True, exist_ok=True)
    tile_path = tile_dir / f"{y}.png"
    if tile_path.exists():
        return tile_path
    req = urllib.request.Request(
        OSM_TILE_URL.format(z=z, x=x, y=y),
        headers={
            "User-Agent": "icefall-thesis-study-area-map/1.0 (master thesis figure; contact: C. Wydra)",
            "Referer": "https://www.openstreetmap.org/",
        },
    )
    with urllib.request.urlopen(req, timeout=45) as resp:
        tile_path.write_bytes(resp.read())
    return tile_path


def osm_tile_mercator_bounds(x_min: int, y_min: int, x_max: int, y_max: int, zoom: int) -> tuple[float, float, float, float]:
    radius = 6378137.0
    origin = math.pi * radius
    tile_span = 2 * origin / (2**zoom)
    left = -origin + x_min * tile_span
    right = -origin + (x_max + 1) * tile_span
    top = origin - y_min * tile_span
    bottom = origin - (y_max + 1) * tile_span
    return left, bottom, right, top


def osm_standard_background(
    extent: tuple[float, float, float, float],
    zoom: int = OSM_STANDARD_ZOOM,
    max_px: int = 2600,
) -> tuple[np.ndarray, int]:
    lon_min, lat_min, lon_max, lat_max = extent_to_lonlat_bbox(extent)
    x0, y_top = lonlat_to_osm_tile(lon_min, lat_max, zoom)
    x1, y_bottom = lonlat_to_osm_tile(lon_max, lat_min, zoom)
    x_min, x_max = min(x0, x1), max(x0, x1)
    y_min, y_max = min(y_top, y_bottom), max(y_top, y_bottom)
    tile_count = (x_max - x_min + 1) * (y_max - y_min + 1)

    mosaic = Image.new("RGB", ((x_max - x_min + 1) * 256, (y_max - y_min + 1) * 256), "white")
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            tile = Image.open(fetch_osm_tile(zoom, x, y)).convert("RGB")
            mosaic.paste(tile, ((x - x_min) * 256, (y - y_min) * 256))

    src = np.asarray(mosaic).transpose(2, 0, 1)
    src_bounds = osm_tile_mercator_bounds(x_min, y_min, x_max, y_max, zoom)
    src_transform = transform_from_bounds(*src_bounds, width=mosaic.width, height=mosaic.height)

    xmin, ymin, xmax, ymax = extent
    aspect = (xmax - xmin) / (ymax - ymin)
    out_w = max_px
    out_h = max(450, int(round(max_px / aspect)))
    dst = np.zeros((3, out_h, out_w), dtype=np.uint8)
    dst_transform = transform_from_bounds(xmin, ymin, xmax, ymax, width=out_w, height=out_h)
    reproject(
        source=src,
        destination=dst,
        src_transform=src_transform,
        src_crs="EPSG:3857",
        dst_transform=dst_transform,
        dst_crs=MAP_CRS,
        resampling=Resampling.bilinear,
    )
    rgba = np.moveaxis(dst, 0, -1)
    alpha = np.full((out_h, out_w, 1), 255, dtype=np.uint8)
    return np.concatenate([rgba, alpha], axis=2), tile_count


def draw_osm_standard_background(ax, extent: tuple[float, float, float, float], zoom: int = OSM_STANDARD_ZOOM) -> int:
    rgba, tile_count = osm_standard_background(extent, zoom=zoom)
    ax.imshow(rgba, extent=(extent[0], extent[2], extent[1], extent[3]), origin="upper", zorder=1, interpolation="bilinear")
    ax.set_facecolor("#f4f3ef")
    return tile_count


def osm_standard_mosaic_3857(
    extent: tuple[float, float, float, float],
    zoom: int = OSM_STANDARD_ZOOM,
) -> tuple[np.ndarray, tuple[float, float, float, float], int]:
    lon_min, lat_min, lon_max, lat_max = extent_3857_to_lonlat_bbox(extent)
    x0, y_top = lonlat_to_osm_tile(lon_min, lat_max, zoom)
    x1, y_bottom = lonlat_to_osm_tile(lon_max, lat_min, zoom)
    x_min, x_max = min(x0, x1), max(x0, x1)
    y_min, y_max = min(y_top, y_bottom), max(y_top, y_bottom)
    tile_count = (x_max - x_min + 1) * (y_max - y_min + 1)

    mosaic = Image.new("RGB", ((x_max - x_min + 1) * 256, (y_max - y_min + 1) * 256), "white")
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            tile = Image.open(fetch_osm_tile(zoom, x, y)).convert("RGB")
            mosaic.paste(tile, ((x - x_min) * 256, (y - y_min) * 256))

    return np.asarray(mosaic), osm_tile_mercator_bounds(x_min, y_min, x_max, y_max, zoom), tile_count


def draw_osm_standard_background_3857(
    ax,
    extent: tuple[float, float, float, float],
    zoom: int = OSM_STANDARD_ZOOM,
) -> int:
    image, image_extent, tile_count = osm_standard_mosaic_3857(extent, zoom=zoom)
    ax.imshow(
        image,
        extent=(image_extent[0], image_extent[2], image_extent[1], image_extent[3]),
        origin="upper",
        zorder=1,
        interpolation="bilinear",
    )
    ax.set_facecolor("#f4f3ef")
    return tile_count


def overpass_bbox(extent: tuple[float, float, float, float], pad_deg: float = 0.04) -> tuple[float, float, float, float]:
    lon_min, lat_min, lon_max, lat_max = extent_to_lonlat_bbox(extent)
    return lat_min - pad_deg, lon_min - pad_deg, lat_max + pad_deg, lon_max + pad_deg


def fetch_osm_features(
    extent: tuple[float, float, float, float],
    cache_name: str,
    detail: bool = False,
) -> tuple[list[dict], str | None]:
    """Fetch a small, cached OpenStreetMap vector context layer via Overpass."""
    cache_path = CACHE_DIR / f"osm_{cache_name}_vector.json"
    if not cache_path.exists():
        south, west, north, east = overpass_bbox(extent)
        highway_re = "^(motorway|trunk|primary|secondary)$"
        query = f"""
[out:json][timeout:120];
(
  way["waterway"~"^(river|canal)$"]({south:.5f},{west:.5f},{north:.5f},{east:.5f});
  way["highway"~"{highway_re}"]({south:.5f},{west:.5f},{north:.5f},{east:.5f});
  way["railway"="rail"]({south:.5f},{west:.5f},{north:.5f},{east:.5f});
);
out geom;
        """
        req = urllib.request.Request(
            OVERPASS_URL,
            data=urllib.parse.urlencode({"data": query}).encode("utf-8"),
            headers={
                "User-Agent": "icefall-thesis-study-area-map/1.0",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                cache_path.write_bytes(resp.read())
        except Exception as exc:  # pragma: no cover - keeps map reproducible offline if OSM is unavailable.
            return [], f"OpenStreetMap Overpass vector background could not be fetched: {exc}"

    data = json.loads(cache_path.read_text(encoding="utf-8"))
    elements = [el for el in data.get("elements", []) if el.get("type") == "way" and el.get("geometry")]
    return elements, None


def projected_osm_geometry(feature: dict) -> tuple[np.ndarray, np.ndarray, float] | None:
    geometry = feature.get("geometry", [])
    if len(geometry) < 2:
        return None
    lons = np.asarray([point["lon"] for point in geometry], dtype=float)
    lats = np.asarray([point["lat"] for point in geometry], dtype=float)
    xs, ys = TO_3035.transform(lons, lats)
    xs = np.asarray(xs, dtype=float)
    ys = np.asarray(ys, dtype=float)
    length_m = float(np.hypot(np.diff(xs), np.diff(ys)).sum())
    return xs, ys, length_m


def keep_osm_feature(feature: dict, length_m: float, detail: bool = False) -> bool:
    tags = feature.get("tags", {})
    if "waterway" in tags:
        return tags.get("waterway") in {"river", "canal"} and length_m >= (3000 if detail else 3500)
    if tags.get("railway") == "rail":
        return length_m >= (1400 if detail else 1000)
    highway = str(tags.get("highway", ""))
    if highway not in {"motorway", "trunk", "primary", "secondary"}:
        return False
    if highway == "secondary" and not detail:
        return length_m >= 1200
    return length_m >= (1000 if detail else 800)


def count_drawn_osm_features(features: list[dict], detail: bool = False) -> int:
    total = 0
    for feature in features:
        projected = projected_osm_geometry(feature)
        if projected is None:
            continue
        if keep_osm_feature(feature, projected[2], detail=detail):
            total += 1
    return total


def draw_osm_features(ax, features: list[dict], detail: bool = False) -> None:
    for feature in features:
        tags = feature.get("tags", {})
        projected = projected_osm_geometry(feature)
        if projected is None:
            continue
        xs, ys, length_m = projected
        if not keep_osm_feature(feature, length_m, detail=detail):
            continue

        if "waterway" in tags:
            ax.plot(
                xs,
                ys,
                color="#477b9e",
                lw=0.68 if detail else 0.58,
                alpha=0.62,
                solid_capstyle="round",
                zorder=3.05,
            )
            continue

        if tags.get("railway") == "rail":
            ax.plot(
                xs,
                ys,
                color="#5f625f",
                lw=0.42 if detail else 0.34,
                alpha=0.42,
                linestyle=(0, (4, 3)),
                solid_capstyle="round",
                zorder=3.18,
            )
            continue

        highway = str(tags.get("highway", ""))
        widths = {
            "motorway": 1.25 if detail else 1.02,
            "trunk": 1.05 if detail else 0.88,
            "primary": 0.82 if detail else 0.68,
            "secondary": 0.52,
        }
        colors = {
            "motorway": "#8a6d43",
            "trunk": "#94764b",
            "primary": "#9f855f",
            "secondary": "#aa9b80",
        }
        if highway in widths:
            ax.plot(
                xs,
                ys,
                color="white",
                lw=widths[highway] + (0.65 if highway in {"motorway", "trunk"} else 0.45),
                alpha=0.50 if highway != "secondary" else 0.38,
                solid_capstyle="round",
                zorder=3.08,
            )
            ax.plot(
                xs,
                ys,
                color=colors[highway],
                lw=widths[highway],
                alpha=0.54 if highway == "secondary" else 0.66,
                solid_capstyle="round",
                zorder=3.2,
            )


def draw_boundaries(ax, extent: tuple[float, float, float, float], countries: Path, admin1: Path) -> None:
    bbox_lonlat = extent_to_lonlat_bbox(extent)
    for _rec, xs, ys in iter_shape_parts(countries, bbox_lonlat):
        ax.plot(xs, ys, color="#343a36", lw=0.55, alpha=0.52, zorder=4)
    for rec, xs, ys in iter_shape_parts(admin1, bbox_lonlat):
        admin = str(rec.get("admin", ""))
        name = str(rec.get("name", ""))
        if admin == "Austria" and name in {"Tirol", "Salzburg", "Vorarlberg", "Carinthia"}:
            ax.plot(xs, ys, color="#1f2722", lw=1.05 if name == "Tirol" else 0.6, alpha=0.78, zorder=5)
        else:
            ax.plot(xs, ys, color="#343a36", lw=0.35, alpha=0.20, zorder=4)


def iter_shape_parts_3857(shp_path: Path, bbox_lonlat: tuple[float, float, float, float]):
    reader = shapefile.Reader(str(shp_path), encoding="latin1")
    fields = [field[0] for field in reader.fields[1:]]
    for shape_record in reader.iterShapeRecords():
        shp = shape_record.shape
        if not shape_intersects_bbox(shp.bbox, bbox_lonlat):
            continue
        rec = dict(zip(fields, shape_record.record))
        pts = np.asarray(shp.points, dtype=float)
        if len(pts) == 0:
            continue
        parts = list(shp.parts) + [len(pts)]
        for start, end in zip(parts[:-1], parts[1:]):
            part = pts[start:end]
            if len(part) < 2:
                continue
            xs, ys = TO_3857.transform(part[:, 0], part[:, 1])
            yield rec, np.asarray(xs), np.asarray(ys)


def draw_boundaries_3857(ax, extent: tuple[float, float, float, float], countries: Path, admin1: Path) -> None:
    bbox_lonlat = extent_3857_to_lonlat_bbox(extent)
    for _rec, xs, ys in iter_shape_parts_3857(countries, bbox_lonlat):
        ax.plot(xs, ys, color="#2a312c", lw=0.45, alpha=0.38, zorder=4)
    for rec, xs, ys in iter_shape_parts_3857(admin1, bbox_lonlat):
        admin = str(rec.get("admin", ""))
        name = str(rec.get("name", ""))
        if admin == "Austria" and name in {"Tirol", "Salzburg", "Vorarlberg", "Carinthia"}:
            ax.plot(xs, ys, color="#18231d", lw=1.0 if name == "Tirol" else 0.55, alpha=0.78, zorder=5)
        else:
            ax.plot(xs, ys, color="#2a312c", lw=0.30, alpha=0.16, zorder=4)


def read_dem_rgb(
    extent: tuple[float, float, float, float],
    max_px: int = 1700,
) -> tuple[np.ndarray, np.ndarray, tuple[float, float, float, float]]:
    xmin, ymin, xmax, ymax = extent
    with rasterio.open(DEM_PATH) as ds:
        window = from_bounds(xmin, ymin, xmax, ymax, ds.transform)
        window = window.round_offsets().round_lengths()
        aspect = (xmax - xmin) / (ymax - ymin)
        if aspect >= 1:
            out_w = max_px
            out_h = max(240, int(round(max_px / aspect)))
        else:
            out_h = max_px
            out_w = max(240, int(round(max_px * aspect)))
        elev = ds.read(
            1,
            window=window,
            out_shape=(out_h, out_w),
            resampling=Resampling.bilinear,
            masked=True,
        ).astype("float64")
    data = np.asarray(elev.filled(np.nan), dtype=float)
    finite = np.isfinite(data)
    if not finite.any():
        raise ValueError("No valid DEM cells in requested extent.")
    fill = float(np.nanmedian(data))
    data_filled = np.where(finite, data, fill)
    low, high = np.nanpercentile(data_filled[finite], [2, 98])
    if not np.isfinite(low) or not np.isfinite(high) or low == high:
        low, high = float(np.nanmin(data_filled[finite])), float(np.nanmax(data_filled[finite]))
    data_clip = np.clip(data_filled, low, high)

    cmap = LinearSegmentedColormap.from_list(
        "muted_alpine",
        [
            "#dce8d2",
            "#c4d2b6",
            "#d0c7b0",
            "#aaa79a",
            "#f2f1ec",
        ],
    )
    ls = LightSource(azdeg=315, altdeg=45)
    cell_x = max((xmax - xmin) / data_clip.shape[1], 1)
    cell_y = max((ymax - ymin) / data_clip.shape[0], 1)
    shade = ls.hillshade(data_clip, vert_exag=3.1, dx=cell_x, dy=cell_y, fraction=1.55)
    elev_norm = Normalize(vmin=low, vmax=high)
    base = cmap(elev_norm(data_clip))
    rgb = base.copy()
    rgb[:, :, :3] = np.clip(base[:, :, :3] * (0.50 + 0.68 * shade[:, :, None]), 0, 1)
    rgb[:, :, 3] = 1.0
    rgb[~finite, :3] = (0.96, 0.96, 0.94)
    rgb[~finite, 3] = 1.0
    return rgb, np.where(finite, data_filled, np.nan), extent


def draw_dem(ax, extent: tuple[float, float, float, float], max_px: int) -> None:
    rgb, dem, image_extent = read_dem_rgb(extent, max_px=max_px)
    ax.imshow(rgb, extent=(image_extent[0], image_extent[2], image_extent[1], image_extent[3]), origin="upper", zorder=1, interpolation="bilinear")
    ny, nx = dem.shape
    xs = np.linspace(image_extent[0], image_extent[2], nx)
    ys = np.linspace(image_extent[3], image_extent[1], ny)
    minor_levels = np.arange(250, 4001, 250)
    ax.contour(
        xs,
        ys,
        dem,
        levels=minor_levels,
        colors="#6e756d",
        linewidths=0.13,
        alpha=0.13,
        zorder=2.35,
    )
    levels = np.arange(500, 4001, 500)
    ax.contour(
        xs,
        ys,
        dem,
        levels=levels,
        colors="#5b635c",
        linewidths=0.30,
        alpha=0.31,
        zorder=2.4,
    )
    ax.set_facecolor("#f5f5ef")


def draw_graticule(
    ax,
    extent: tuple[float, float, float, float],
    lon_step: float,
    lat_step: float,
    fontsize: int = 7,
) -> None:
    lon_min, lat_min, lon_max, lat_max = extent_to_lonlat_bbox(extent)
    lon_start = math.floor(lon_min / lon_step) * lon_step
    lon_end = math.ceil(lon_max / lon_step) * lon_step
    lat_start = math.floor(lat_min / lat_step) * lat_step
    lat_end = math.ceil(lat_max / lat_step) * lat_step
    path_effect = [pe.withStroke(linewidth=2.2, foreground="white", alpha=0.8)]

    for lon in np.arange(lon_start, lon_end + lon_step / 2, lon_step):
        lats = np.linspace(lat_min, lat_max, 160)
        lons = np.full_like(lats, lon)
        xs, ys = TO_3035.transform(lons, lats)
        ax.plot(xs, ys, color="white", lw=0.42, alpha=0.34, zorder=2)
        visible = (xs >= extent[0]) & (xs <= extent[2]) & (ys >= extent[1]) & (ys <= extent[3])
        if visible.any():
            idx = np.where(visible)[0][0]
            ax.text(xs[idx], extent[1] + 0.012 * (extent[3] - extent[1]), f"{lon:.1f}E",
                    fontsize=fontsize, color="#3d4640", ha="center", va="bottom",
                    path_effects=path_effect, zorder=9)

    for lat in np.arange(lat_start, lat_end + lat_step / 2, lat_step):
        lons = np.linspace(lon_min, lon_max, 160)
        lats = np.full_like(lons, lat)
        xs, ys = TO_3035.transform(lons, lats)
        ax.plot(xs, ys, color="white", lw=0.42, alpha=0.34, zorder=2)
        visible = (xs >= extent[0]) & (xs <= extent[2]) & (ys >= extent[1]) & (ys <= extent[3])
        if visible.any():
            idx = np.where(visible)[0][0]
            ax.text(extent[0] + 0.012 * (extent[2] - extent[0]), ys[idx], f"{lat:.1f}N",
                    fontsize=fontsize, color="#3d4640", ha="left", va="center",
                    path_effects=path_effect, zorder=9)


def draw_graticule_3857(
    ax,
    extent: tuple[float, float, float, float],
    lon_step: float,
    lat_step: float,
    fontsize: int = 7,
) -> None:
    lon_min, lat_min, lon_max, lat_max = extent_3857_to_lonlat_bbox(extent)
    lon_start = math.floor(lon_min / lon_step) * lon_step
    lon_end = math.ceil(lon_max / lon_step) * lon_step
    lat_start = math.floor(lat_min / lat_step) * lat_step
    lat_end = math.ceil(lat_max / lat_step) * lat_step
    path_effect = [pe.withStroke(linewidth=2.2, foreground="white", alpha=0.78)]

    for lon in np.arange(lon_start, lon_end + lon_step / 2, lon_step):
        lats = np.linspace(lat_min, lat_max, 160)
        lons = np.full_like(lats, lon)
        xs, ys = TO_3857.transform(lons, lats)
        ax.plot(xs, ys, color="white", lw=0.42, alpha=0.30, zorder=2)
        visible = (xs >= extent[0]) & (xs <= extent[2]) & (ys >= extent[1]) & (ys <= extent[3])
        if visible.any():
            idx = np.where(visible)[0][0]
            ax.text(
                xs[idx],
                extent[1] + 0.012 * (extent[3] - extent[1]),
                f"{lon:.1f}E",
                fontsize=fontsize,
                color="#3d4640",
                ha="center",
                va="bottom",
                path_effects=path_effect,
                zorder=9,
            )

    for lat in np.arange(lat_start, lat_end + lat_step / 2, lat_step):
        lons = np.linspace(lon_min, lon_max, 160)
        lats = np.full_like(lons, lat)
        xs, ys = TO_3857.transform(lons, lats)
        ax.plot(xs, ys, color="white", lw=0.42, alpha=0.30, zorder=2)
        visible = (xs >= extent[0]) & (xs <= extent[2]) & (ys >= extent[1]) & (ys <= extent[3])
        if visible.any():
            idx = np.where(visible)[0][0]
            ax.text(
                extent[0] + 0.012 * (extent[2] - extent[0]),
                ys[idx],
                f"{lat:.1f}N",
                fontsize=fontsize,
                color="#3d4640",
                ha="left",
                va="center",
                path_effects=path_effect,
                zorder=9,
            )


def add_scale_bar(
    ax,
    length_km: int,
    label: str | None = None,
    x_frac: float = 0.07,
    y_frac: float = 0.065,
) -> None:
    xmin, xmax = ax.get_xlim()
    ymin, ymax = ax.get_ylim()
    width = xmax - xmin
    height = ymax - ymin
    x0 = xmin + x_frac * width
    y0 = ymin + y_frac * height
    x1 = x0 + length_km * 1000
    ax.plot([x0, x1], [y0, y0], color="#1d2520", lw=2.4, solid_capstyle="butt", zorder=10)
    ax.plot([x0, x0], [y0 - 0.012 * height, y0 + 0.012 * height], color="#1d2520", lw=1.8, zorder=10)
    ax.plot([x1, x1], [y0 - 0.012 * height, y0 + 0.012 * height], color="#1d2520", lw=1.8, zorder=10)
    ax.text(x0, y0 - 0.023 * height, "0", ha="center", va="top", fontsize=8, color="#1d2520", zorder=10)
    ax.text(x1, y0 - 0.023 * height, label or f"{length_km} km", ha="center", va="top", fontsize=8, color="#1d2520", zorder=10)


def add_scale_bar_3857(
    ax,
    length_km: int,
    label: str | None = None,
    x_frac: float = 0.07,
    y_frac: float = 0.065,
) -> None:
    xmin, xmax = ax.get_xlim()
    ymin, ymax = ax.get_ylim()
    width = xmax - xmin
    height = ymax - ymin
    x0 = xmin + x_frac * width
    y0 = ymin + y_frac * height
    lon, lat = FROM_3857.transform(x0, y0)
    mercator_length = length_km * 1000 / max(math.cos(math.radians(lat)), 0.2)
    x1 = x0 + mercator_length
    ax.plot([x0, x1], [y0, y0], color="#1d2520", lw=2.4, solid_capstyle="butt", zorder=10)
    ax.plot([x0, x0], [y0 - 0.012 * height, y0 + 0.012 * height], color="#1d2520", lw=1.8, zorder=10)
    ax.plot([x1, x1], [y0 - 0.012 * height, y0 + 0.012 * height], color="#1d2520", lw=1.8, zorder=10)
    ax.text(x0, y0 - 0.023 * height, "0", ha="center", va="top", fontsize=8, color="#1d2520", zorder=10)
    ax.text(x1, y0 - 0.023 * height, label or f"{length_km} km", ha="center", va="top", fontsize=8, color="#1d2520", zorder=10)


def add_true_north_arrow(ax) -> None:
    xmin, xmax = ax.get_xlim()
    ymin, ymax = ax.get_ylim()
    width = xmax - xmin
    height = ymax - ymin
    x = xmin + 0.93 * width
    y = ymin + 0.84 * height
    lon, lat = TO_WGS84.transform(x, y)
    x2, y2 = TO_3035.transform(lon, lat + 0.18)
    dx, dy = x2 - x, y2 - y
    norm = math.hypot(dx, dy)
    if norm == 0:
        dx, dy = 0, 1
        norm = 1
    length = 0.07 * height
    ax.annotate(
        "",
        xy=(x + dx / norm * length, y + dy / norm * length),
        xytext=(x, y),
        arrowprops=dict(arrowstyle="-|>", color="#1b211d", lw=1.8, mutation_scale=14),
        zorder=10,
    )
    ax.text(x, y - 0.018 * height, "N", ha="center", va="top", fontsize=9, fontweight="bold", color="#1b211d", zorder=10)


def add_true_north_arrow_3857(ax) -> None:
    xmin, xmax = ax.get_xlim()
    ymin, ymax = ax.get_ylim()
    width = xmax - xmin
    height = ymax - ymin
    x = xmin + 0.93 * width
    y = ymin + 0.84 * height
    length = 0.07 * height
    ax.annotate(
        "",
        xy=(x, y + length),
        xytext=(x, y),
        arrowprops=dict(arrowstyle="-|>", color="#1b211d", lw=1.8, mutation_scale=14),
        zorder=10,
    )
    ax.text(x, y - 0.018 * height, "N", ha="center", va="top", fontsize=9, fontweight="bold", color="#1b211d", zorder=10)


def draw_panel_frame_for_bbox(ax, lonlat_bbox: tuple[float, float, float, float]) -> None:
    lon_min, lat_min, lon_max, lat_max = lonlat_bbox
    lons = np.concatenate(
        [
            np.linspace(lon_min, lon_max, 80),
            np.full(80, lon_max),
            np.linspace(lon_max, lon_min, 80),
            np.full(80, lon_min),
        ]
    )
    lats = np.concatenate(
        [
            np.full(80, lat_min),
            np.linspace(lat_min, lat_max, 80),
            np.full(80, lat_max),
            np.linspace(lat_max, lat_min, 80),
        ]
    )
    xs, ys = TO_3035.transform(lons, lats)
    ax.plot(xs, ys, color="#202622", lw=0.85, ls=(0, (4, 3)), alpha=0.72, zorder=8)


def draw_panel_frame_for_bbox_3857(ax, lonlat_bbox: tuple[float, float, float, float]) -> None:
    lon_min, lat_min, lon_max, lat_max = lonlat_bbox
    lons = np.concatenate(
        [
            np.linspace(lon_min, lon_max, 80),
            np.full(80, lon_max),
            np.linspace(lon_max, lon_min, 80),
            np.full(80, lon_min),
        ]
    )
    lats = np.concatenate(
        [
            np.full(80, lat_min),
            np.linspace(lat_min, lat_max, 80),
            np.full(80, lat_max),
            np.linspace(lat_max, lat_min, 80),
        ]
    )
    xs, ys = TO_3857.transform(lons, lats)
    ax.plot(xs, ys, color="#202622", lw=0.85, ls=(0, (4, 3)), alpha=0.72, zorder=8)


def draw_points(ax, points: pd.DataFrame, norm: Normalize, size: float, title: str) -> None:
    sc = ax.scatter(
        points["x_3035"],
        points["y_3035"],
        c=points["icefall_elev_m"],
        cmap="viridis",
        norm=norm,
        s=size,
        edgecolors="white",
        linewidths=0.35,
        alpha=0.93,
        zorder=7,
    )
    ax.set_title(title, loc="left", fontsize=10, fontweight="bold", pad=4)
    return sc


def draw_points_3857(ax, points: pd.DataFrame, norm: Normalize, size: float, title: str) -> None:
    sc = ax.scatter(
        points["x_3857"],
        points["y_3857"],
        c=points["icefall_elev_m"],
        cmap="viridis",
        norm=norm,
        s=size,
        edgecolors="white",
        linewidths=0.35,
        alpha=0.94,
        zorder=7,
    )
    ax.set_title(title, loc="left", fontsize=10, fontweight="bold", pad=4)
    return sc


def clean_axes(ax, extent: tuple[float, float, float, float]) -> None:
    ax.set_xlim(extent[0], extent[2])
    ax.set_ylim(extent[1], extent[3])
    ax.set_aspect("equal")
    ax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    for spine in ax.spines.values():
        spine.set_linewidth(0.7)
        spine.set_color("#566057")


def draw_locator(ax, countries: Path, admin1: Path, main_extent: tuple[float, float, float, float]) -> None:
    locator_extent = projected_bbox_from_lonlat(5.0, 43.7, 17.8, 50.0)
    clean_axes(ax, locator_extent)
    ax.set_facecolor("#f3f3ee")
    bbox = extent_to_lonlat_bbox(locator_extent)
    for rec, xs, ys in iter_shape_parts(countries, bbox):
        admin = str(rec.get("ADMIN", rec.get("admin", "")))
        if admin == "Austria":
            ax.fill(xs, ys, color="#d7dfcf", alpha=1.0, zorder=2)
            ax.plot(xs, ys, color="#2d342f", lw=0.7, zorder=3)
        else:
            ax.fill(xs, ys, color="#efeee8", alpha=1.0, zorder=1)
            ax.plot(xs, ys, color="#9a9f99", lw=0.35, zorder=2)

    for rec, xs, ys in iter_shape_parts(admin1, bbox):
        if str(rec.get("admin", "")) == "Austria" and str(rec.get("name", "")) == "Tirol":
            ax.fill(xs, ys, color="#b5c9a5", alpha=0.95, zorder=4)
            ax.plot(xs, ys, color="#202622", lw=0.8, zorder=5)

    rect = mpatches.Rectangle(
        (main_extent[0], main_extent[1]),
        main_extent[2] - main_extent[0],
        main_extent[3] - main_extent[1],
        fill=False,
        edgecolor="#d54f2a",
        linewidth=1.0,
        zorder=6,
    )
    ax.add_patch(rect)
    ax.set_title("Locator", loc="left", fontsize=8, fontweight="bold", pad=2)
    ax.text(0.03, 0.04, "Austria / Alps", transform=ax.transAxes, fontsize=6.5, color="#2f3832")


def draw_locator_for_lonlat_bbox(
    ax,
    countries: Path,
    admin1: Path,
    main_bbox_lonlat: tuple[float, float, float, float],
) -> None:
    locator_extent = projected_bbox_from_lonlat(5.0, 43.7, 17.8, 50.0)
    clean_axes(ax, locator_extent)
    ax.set_facecolor("#f3f3ee")
    bbox = extent_to_lonlat_bbox(locator_extent)
    for rec, xs, ys in iter_shape_parts(countries, bbox):
        admin = str(rec.get("ADMIN", rec.get("admin", "")))
        if admin == "Austria":
            ax.fill(xs, ys, color="#d7dfcf", alpha=1.0, zorder=2)
            ax.plot(xs, ys, color="#2d342f", lw=0.7, zorder=3)
        else:
            ax.fill(xs, ys, color="#efeee8", alpha=1.0, zorder=1)
            ax.plot(xs, ys, color="#9a9f99", lw=0.35, zorder=2)

    for rec, xs, ys in iter_shape_parts(admin1, bbox):
        if str(rec.get("admin", "")) == "Austria" and str(rec.get("name", "")) == "Tirol":
            ax.fill(xs, ys, color="#b5c9a5", alpha=0.95, zorder=4)
            ax.plot(xs, ys, color="#202622", lw=0.8, zorder=5)

    lon_min, lat_min, lon_max, lat_max = main_bbox_lonlat
    rect_extent = projected_bbox_from_lonlat(lon_min, lat_min, lon_max, lat_max)
    rect = mpatches.Rectangle(
        (rect_extent[0], rect_extent[1]),
        rect_extent[2] - rect_extent[0],
        rect_extent[3] - rect_extent[1],
        fill=False,
        edgecolor="#d54f2a",
        linewidth=1.0,
        zorder=6,
    )
    ax.add_patch(rect)
    ax.set_title("Locator", loc="left", fontsize=8, fontweight="bold", pad=2)
    ax.text(0.03, 0.04, "Austria / Alps", transform=ax.transAxes, fontsize=6.5, color="#2f3832")


def add_labels_for_orientation(ax) -> None:
    labels = [
        ("Innsbruck", 11.4041, 47.2692),
        ("Kufstein", 12.1690, 47.5833),
        ("Lienz", 12.7627, 46.8297),
        ("Mayrhofen", 11.8630, 47.1667),
        ("Sölden", 11.0076, 46.9690),
    ]
    for name, lon, lat in labels:
        x, y = TO_3035.transform(lon, lat)
        ax.text(
            x,
            y,
            name,
            fontsize=7,
            color="#2f3832",
            ha="center",
            va="center",
            zorder=8,
            path_effects=[pe.withStroke(linewidth=2.6, foreground="white", alpha=0.85)],
        )


def build_qa(assign: pd.DataFrame, raw: pd.DataFrame, models: set[int]) -> dict[str, object]:
    assigned_uids = {int(v) for v in assign["uid"].dropna().astype(int)}
    raw_uids = {int(v) for v in raw["uid"].dropna().astype(int)}
    raw_valid = raw[np.isfinite(raw["latitude_num"]) & np.isfinite(raw["longitude_num"])]
    raw_valid_uids = {int(v) for v in raw_valid["uid"].dropna().astype(int)}

    model_in_assignment = sorted(models & assigned_uids)
    missing_model = sorted(assigned_uids - models)
    stale_model = sorted(models - assigned_uids)

    coord_compare = assign.merge(
        raw[["uid", "latitude_num", "longitude_num", "elev_num"]],
        on="uid",
        how="left",
    )
    coord_compare["coord_delta_deg"] = np.sqrt(
        (coord_compare["ice_lat"] - coord_compare["latitude_num"]) ** 2
        + (coord_compare["ice_lon"] - coord_compare["longitude_num"]) ** 2
    )
    coord_mismatches = coord_compare[coord_compare["coord_delta_deg"] > 0.0002].copy()

    return {
        "assigned_count": int(len(assign)),
        "assigned_unique_uid_count": int(len(assigned_uids)),
        "valid_assigned_coord_count": int((np.isfinite(assign["ice_lat"]) & np.isfinite(assign["ice_lon"])).sum()),
        "raw_count": int(len(raw)),
        "raw_valid_coord_count": int(len(raw_valid)),
        "assigned_missing_in_raw": sorted(assigned_uids - raw_uids),
        "raw_valid_not_plotted_count": int(len(raw_valid_uids - assigned_uids)),
        "model_uid_count": int(len(models)),
        "model_in_assignment_count": int(len(model_in_assignment)),
        "missing_model_count": int(len(missing_model)),
        "missing_model_sample": missing_model[:80],
        "stale_model_uids": stale_model,
        "coord_mismatch_count": int(len(coord_mismatches)),
        "coord_mismatch_sample": coord_mismatches[["uid", "icefall_name", "coord_delta_deg"]]
        .sort_values("coord_delta_deg", ascending=False)
        .head(20)
        .to_dict("records"),
        "elev_min": float(assign["icefall_elev_m"].min()),
        "elev_max": float(assign["icefall_elev_m"].max()),
    }


def write_documentation(outputs: MapOutputs, qa: dict[str, object]) -> None:
    missing_sample = ", ".join(f"{uid:03d}" for uid in qa["missing_model_sample"])
    stale = ", ".join(f"{uid:03d}" for uid in qa["stale_model_uids"]) or "none"
    mismatch_lines = []
    for row in qa["coord_mismatch_sample"]:
        mismatch_lines.append(
            f"- UID {int(row['uid']):03d}: {row['icefall_name']} coordinate delta "
            f"{float(row['coord_delta_deg']):.6f} degrees"
        )
    if not mismatch_lines:
        mismatch_lines.append("- None above the 0.0002 degree QA threshold.")

    caption = (
        "Study-area map of the selected station-assigned waterfall-ice working inventory used for "
        "the modelling workflow. Points represent the operational assignment table, not a complete "
        "inventory of all waterfall-ice routes in Tyrol. Point colour indicates DEM-derived icefall "
        "elevation. The map shows the full Tyrolean and adjacent Alpine working extent; the dashed "
        "rectangle marks the North Tyrol modelling bbox used in the workflow, and the locator inset "
        "shows the position of the mapped extent within Austria and the Alps. The background is the "
        "OpenStreetMap Standard tile layer (c) OpenStreetMap contributors, ODbL, in Web Mercator "
        "(EPSG:3857). Boundaries are from Natural Earth, and icefall coordinates from "
        "data/AWS/icefalls_nearest_station.csv. Coordinates are stored in WGS 84 (EPSG:4326) and "
        "displayed in Web Mercator (EPSG:3857). Map author: C. Wydra."
    )
    methods = (
        "The study area was delineated from the operational station-assigned icefall dataset used by "
        "the implemented model workflow. This region provides a dense set of selected waterfall-ice "
        "sites across strong elevation and valley-geometry gradients and is covered by the meteorological "
        "and topographic input data used by the model. The mapped icefalls are therefore a selected "
        "modelling basis rather than a complete regional census. This limits spatial interpretation to "
        "the coverage of the implemented workflow and prevents conclusions about the full population of "
        "waterfall-ice routes in Tyrol."
    )

    open_points = [
        f"Existing model-output CSVs cover {qa['model_in_assignment_count']} of "
        f"{qa['assigned_count']} station-assigned UIDs; {qa['missing_model_count']} assigned UIDs have "
        "no inspected model-output CSV in data/ModelRuns.",
        f"Stale or inconsistent model-output UIDs outside the assignment table: {stale}.",
        "The exact final thesis production run date for model outputs is not fixed in the inspected files.",
        "The study-area wording remains 'selected working inventory in Tyrol and adjacent Alpine sectors' "
        "unless a stricter administrative Tyrol-only interpretation is chosen later.",
    ]
    open_points.extend(qa.get("osm_issues", []))

    doc = f"""# Study-Area Icefall Inventory Map

## Created map files

- `{outputs.png}`: high-resolution PNG, 300 dpi, Word-ready raster export.
- `{outputs.pdf}`: PDF export with vector text and raster hillshade.
- `{outputs.svg}`: SVG export with vector text and raster hillshade.

## Data used

- `{ASSIGN_PATH}`: final mapped icefall working inventory; all plotted points come from this file.
- `{RAW_PATH}`: cross-check only for UID and coordinate consistency.
- `{DEM_PATH}`: available local DEM; not used in the current OSM Standard tile rendering.
- `{MODEL_DIR}`: QA only, used to identify existing, missing, or stale model outputs.
- Natural Earth 10 m admin-0 and admin-1 boundaries, cached under `{CACHE_DIR}`.
- OpenStreetMap Standard tile layer, (c) OpenStreetMap contributors (ODbL), cached under `{CACHE_DIR}` and plotted in EPSG:3857.

## CRS and projection

The source icefall coordinates are WGS 84 / EPSG:4326. The plotted map uses Web Mercator / EPSG:3857 to preserve the requested one-to-one OpenStreetMap Standard tile background. The scale bar is corrected for the central map latitude.

## QA checks

- Station-assigned rows plotted: {qa['assigned_count']}.
- Unique station-assigned UIDs: {qa['assigned_unique_uid_count']}.
- Valid plotted coordinates: {qa['valid_assigned_coord_count']}.
- Raw coordinate inventory rows: {qa['raw_count']}; raw rows with valid coordinates: {qa['raw_valid_coord_count']}.
- Raw coordinate-bearing UIDs intentionally not plotted: {qa['raw_valid_not_plotted_count']}.
- Assigned UIDs missing from raw inventory: {qa['assigned_missing_in_raw'] or 'none'}.
- Existing model-output UID files: {qa['model_uid_count']}.
- Existing model-output UID files matching assignment table: {qa['model_in_assignment_count']}.
- Assigned UIDs without inspected model output: {qa['missing_model_count']}; sample: {missing_sample}.
- Stale model-output UIDs outside assignment table: {stale}.
- Coordinate mismatches above 0.0002 degrees: {qa['coord_mismatch_count']}.
- Station-assigned UIDs inside the North Tyrol workflow bbox: {qa.get('core_bbox_count', 0)}.
- OSM Standard tile zoom: {qa.get('osm_tile_zoom', 'not set')}.
- OSM Standard tiles used for the main map mosaic: {qa.get('osm_tile_count', 0)}.

## Coordinate mismatch sample

{chr(10).join(mismatch_lines)}

## Cartographic decisions

- The point layer is the station-assigned operational modelling inventory, not the full raw coordinate inventory.
- Icefalls are symbolized by elevation because elevation is methodologically relevant to ice formation, temperature correction, and model forcing.
- The background uses the OpenStreetMap Standard raster tile layer as requested, with visible OSM attribution in the figure source note.
- The OSM tile mosaic is used as background context only; all icefall coordinates remain the project-derived station-assigned inventory.
- The former separate detail and locator panels were collapsed into one main map with an embedded locator inset; the North Tyrol workflow bbox remains as a subtle dashed frame.
- Place labels come from the OpenStreetMap Standard tile layer; no separate icefall labels are added to avoid overcrowding.
- Map author: {MAP_AUTHOR}.

## Suggested figure caption

{caption}

## Suggested methods paragraph

{methods}

## Open points

{chr(10).join(f'- <span style="color:red">{item}</span>' for item in open_points)}
"""
    outputs.doc.write_text(doc, encoding="utf-8")


def build_map() -> MapOutputs:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    assign = read_assignment()
    raw = read_raw_inventory()
    models = model_uids()
    qa = build_qa(assign, raw, models)

    if qa["assigned_count"] != 325:
        raise ValueError(f"Expected 325 station-assigned records, found {qa['assigned_count']}.")
    if qa["valid_assigned_coord_count"] != qa["assigned_count"]:
        raise ValueError("Some station-assigned records have invalid coordinates; not plotting silently.")

    countries = fetch_natural_earth("countries")
    admin1 = fetch_natural_earth("admin1")

    main_extent = point_extent_columns(assign, "x_3857", "y_3857", aspect=1.78, pad_m=45000)
    core_points = assign[
        assign["ice_lon"].between(NORTH_TYROL_BBOX_LONLAT[0], NORTH_TYROL_BBOX_LONLAT[2])
        & assign["ice_lat"].between(NORTH_TYROL_BBOX_LONLAT[1], NORTH_TYROL_BBOX_LONLAT[3])
    ].copy()
    qa["core_bbox_count"] = len(core_points)

    fig = plt.figure(figsize=(11.6, 8.2), dpi=300, constrained_layout=False)
    ax_main = fig.add_axes([0.055, 0.18, 0.89, 0.69])

    norm = Normalize(vmin=math.floor(qa["elev_min"] / 250) * 250, vmax=math.ceil(qa["elev_max"] / 250) * 250)

    qa["osm_tile_zoom"] = OSM_STANDARD_ZOOM
    qa["osm_tile_count"] = draw_osm_standard_background_3857(ax_main, main_extent, zoom=OSM_STANDARD_ZOOM)
    draw_boundaries_3857(ax_main, main_extent, countries, admin1)
    clean_axes(ax_main, main_extent)

    draw_graticule_3857(ax_main, main_extent, lon_step=0.5, lat_step=0.2, fontsize=7)

    draw_panel_frame_for_bbox_3857(ax_main, NORTH_TYROL_BBOX_LONLAT)
    sc = draw_points_3857(
        ax_main,
        assign,
        norm,
        size=13,
        title="Study-area map: station-assigned icefalls (n = 325)",
    )
    add_scale_bar_3857(ax_main, 50)
    add_true_north_arrow_3857(ax_main)

    # Locator inset positioned on the right side below the north arrow.
    # Values are relative to the main map axis: [left, bottom, width, height].
    ax_locator = ax_main.inset_axes([0.735, 0.565, 0.205, 0.205])
    ax_locator.set_zorder(20)
    draw_locator_for_lonlat_bbox(
        ax_locator,
        countries,
        admin1,
        extent_3857_to_lonlat_bbox(main_extent),
    )

    cax = fig.add_axes([0.415, 0.108, 0.42, 0.028])
    cbar = fig.colorbar(sc, cax=cax, orientation="horizontal")
    cbar.set_label("Icefall elevation (m a.s.l.)", fontsize=7.5)
    cbar.ax.tick_params(labelsize=7)

    legend_handles = [
        Line2D([0], [0], marker="o", linestyle="", markerfacecolor="#3b528b", markeredgecolor="white", markeredgewidth=0.5, markersize=6),
        Line2D([0], [0], color="#202622", lw=0.85, ls=(0, (4, 3)), alpha=0.72),
        Line2D([0], [0], color="#303833", lw=0.9),
    ]
    fig.legend(
        legend_handles,
        ["Station-assigned icefall UID", "North Tyrol workflow bbox", "Tyrol boundary"],
        loc="lower left",
        bbox_to_anchor=(0.055, 0.045),
        ncol=1,
        frameon=False,
        fontsize=8,
        handlelength=2.0,
        columnspacing=1.2,
    )
    source_note = (
        f"Map author: {MAP_AUTHOR}. "
        "Display CRS: Web Mercator (EPSG:3857). "
        "Background: OpenStreetMap Standard tile layer © OpenStreetMap contributors "
        "(openstreetmap.org/copyright). "
    )
    fig.text(0.945, 0.012, textwrap.fill(source_note, width=86), ha="right", va="bottom", fontsize=5.9, color="#4b554d")

    png = OUT_DIR / "study_area_icefall_inventory_map.png"
    pdf = OUT_DIR / "study_area_icefall_inventory_map.pdf"
    svg = OUT_DIR / "study_area_icefall_inventory_map.svg"
    doc = OUT_DIR / "study_area_icefall_inventory_map_documentation.md"

    for out in (png, pdf, svg):
        fig.savefig(out, dpi=300)
    plt.close(fig)

    outputs = MapOutputs(png=png, pdf=pdf, svg=svg, doc=doc)
    write_documentation(outputs, qa)
    return outputs


if __name__ == "__main__":
    outputs = build_map()
    print(f"PNG={outputs.png}")
    print(f"PDF={outputs.pdf}")
    print(f"SVG={outputs.svg}")
    print(f"DOC={outputs.doc}")
