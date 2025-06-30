# etl-pipeline

A dock-widget plugin for QGIS 3 that lets you assemble simple Extract-Transform-Load (ETL) workflows without leaving the GIS interface and run them either interactively or head-less via the Processing toolbox.

## Features

### Sources
* CSV (local)
* Parquet (local)
* PostGIS (read-only)
* Cloud object stores (S3 / GCS / Azure) – Parquet

### Transforms
* Row Filter (`pandas` / `GeoPandas` query syntax)
* Reproject (on-the-fly CRS change)
* Join with an external table (CSV / Parquet)

### Sinks
* CSV
* Parquet
* PostGIS (write/replace)
* Cloud object stores (Parquet)

Additional nodes can be added easily—see `etl_builder_panel.py` for examples.

## Quick start

1. Clone or copy this folder into your local QGIS plugin directory, typically:
   * Linux: `~/.local/share/QGIS/QGIS3/profiles/default/python/plugins`
   * Windows: `%APPDATA%\QGIS\QGIS3\profiles\default\python\plugins`
2. In QGIS, enable the plugin via **Plugins ▸ Manage and Install Plugins**.
3. A new toolbar button “ETL Pipeline Builder” will appear and open the dock.
4. Use the toolbar inside the dock to add **Source → Transform … → Sink** steps, then press **Run**.

## Headless execution

Each saved workflow (`*.json`) can be executed outside the GUI:

```bash
qgis_process run etl:run_workflow INPUT=/path/to/workflow.json
```

The Processing provider is registered automatically when the plugin loads.

## Requirements

* QGIS 3.22 +
* Python packages (already bundled with most QGIS builds):
  * `pandas`, `geopandas`, `fsspec`, `pyarrow`, `sqlalchemy`
  * `psycopg2` if you need PostGIS access

If a dependency is missing the plugin shows a clear error message and the relevant node will fail gracefully.

## Development

```bash
pip install -r dev-requirements.txt  # optional linters / black / pytest
pb_tool compile                      # generate resources (if you change icons)
```

To run the plugin from source while developing, use QGIS’s **Plugin Reloader** add-on or start QGIS with the `--code` flag pointing at `main.py`.

## Credits

Created by [Your Name] as part of a data-science tooling initiative. Ideas and iconography inspired by QGIS-native Processing models and Apache Airflow.