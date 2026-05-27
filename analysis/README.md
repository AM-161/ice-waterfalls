# Analysis and thesis helper scripts

This folder contains scripts that are useful for research, QA, and thesis figures, but are not part of the GitHub Pages runtime build.

Runtime build scripts stay in `scripts/`:

- `scripts/00_build_plots_all.R`
- `scripts/01_build_map.R`
- `scripts/02_build_list_page.R`
- `scripts/diagram_uid.R`

The scripts in this folder may generate large cache or output files. Those generated folders are ignored by Git.

## Contents

- `icefall_structure/`: derives route structure, aspect, slope, and QA data.
- `cold_air_pooling/`: derives cold-air-pooling lookup tables.
- `study_area_map/`: creates publication-style thesis map exports.
- `historical_climate/`: case-study scripts for historical climate comparisons.
