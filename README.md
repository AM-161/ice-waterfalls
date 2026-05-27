# Ice Waterfalls

Interactive icefall map and model output for a master thesis project.

## Repository structure

- `static/`: static homepage source.
- `assets/`: static assets copied into the GitHub Pages build.
- `scripts/`: runtime build scripts used by GitHub Actions.
- `data/`: input and small derived data required by the model/build.
- `analysis/`: research, QA, and thesis-helper scripts that are not part of the normal Pages build.
- `site/`: generated GitHub Pages output. It is ignored except for `.gitkeep`.

## GitHub Pages build

The Pages site is built by `.github/workflows/build_site.yml` on pushes to `main` and `main-test`.

The workflow runs:

1. `scripts/00_build_plots_all.R`
2. `scripts/02_build_list_page.R`
3. copy `static/index.html` and `assets/` into `site/`
4. `scripts/01_build_map.R`

On `main`, the generated `site/` folder is deployed to GitHub Pages. On `main-test`, it is uploaded as a workflow artifact for checking.

## Local smoke test

From the repository root:

```bash
Rscript scripts/00_build_plots_all.R --uids=1
Rscript scripts/02_build_list_page.R
Rscript scripts/01_build_map.R
```

Then serve the generated site locally:

```bash
python -m http.server 8000 --directory site
```

## Notes for pushing

Do not commit generated caches, `site/`, local DEM source files, RStudio state, or local analysis outputs. The `.gitignore` keeps these out of Git.

The repository already contains large tracked data files, but no currently tracked file is above GitHub's 100 MB single-file limit.

## Copyright and use

Copyright (c) 2026. All rights reserved.

This repository was created as part of a master thesis. Code, models, scripts, texts, graphics, and other contents are protected by copyright.

Without prior written permission, copying, redistribution, publication, modification, derivative works, use in other projects or teaching material, and commercial use are not permitted.

Use is limited to reading, review, and assessment in the context of the master thesis.
