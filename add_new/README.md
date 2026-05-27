# Add new icefalls

Edit `add_new/new_icefalls.csv`. Add one icefall per row.

- Leave `uid` empty for a new icefall. The script assigns the next free UID.
- Set `uid` to an existing UID to update and recalculate that icefall.
- Required fields are `name`, `latitude`, and `longitude`.
- Decimal comma and decimal point both work for coordinates.

From the repository root run:

```bash
Rscript scripts/add_new_icefalls.R
```

On this Windows machine `Rscript` may not be in `PATH`; this equivalent command works:

```powershell
& "C:\Program Files\R\R-4.5.3\bin\Rscript.exe" scripts/add_new_icefalls.R
```

Useful checks:

```bash
Rscript scripts/add_new_icefalls.R --dry-run
Rscript scripts/add_new_icefalls.R --run-models
Rscript scripts/add_new_icefalls.R --build-map
```

Default recalculation updates the main icefall table, DEM height/aspect,
weather-station assignment, wind vulnerability, topographic sun tables, route
structure, cold-air-pooling tables, and the list-page table.
