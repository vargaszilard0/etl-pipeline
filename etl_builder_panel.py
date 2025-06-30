"""Dock widget hosting ETL graph editor."""

import os
import json
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
import fsspec
from qgis.PyQt import uic, QtWidgets, QtCore
from qgis.core import QgsTask

FORM_CLASS, _ = uic.loadUiType(os.path.join(os.path.dirname(__file__), "etl_builder_base.ui"))


class ETLBuilderPanel(QtWidgets.QDockWidget, FORM_CLASS):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("ETLBuilderDock")
        self.setupUi(self)
        self.setWindowTitle("ETL Pipeline Builder")

        self.actionRun.triggered.connect(self.run_workflow)
        self.actionOpen.triggered.connect(self.load_workflow)
        self.actionSave.triggered.connect(self.save_workflow)
        self.actionAddSource.triggered.connect(self.add_source)
        self.actionAddFilter.triggered.connect(self.add_filter)
        self.actionAddSink.triggered.connect(self.add_sink)
        if hasattr(self, 'actionAddParquetSink'):
            self.actionAddParquetSink.triggered.connect(self.add_parquet_sink)
        if hasattr(self, 'actionAddPostgisSink'):
            self.actionAddPostgisSink.triggered.connect(self.add_postgis_sink)
        if hasattr(self, 'actionAddCloudSource'):
            self.actionAddCloudSource.triggered.connect(self.add_cloud_source)
        if hasattr(self, 'actionAddCloudSink'):
            self.actionAddCloudSink.triggered.connect(self.add_cloud_sink)
        if hasattr(self, 'actionAddParquetSource'):
            self.actionAddParquetSource.triggered.connect(self.add_parquet_source)
        if hasattr(self, 'actionAddPostgisSource'):
            self.actionAddPostgisSource.triggered.connect(self.add_postgis_source)

        self.steps: list[dict] = []

        self.listSteps.currentRowChanged.connect(self.load_params_for_row)

    def add_parquet_source(self):
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Parquet source", "", "Parquet Files (*.parquet *.pq)")
        if not path:
            return
        step = {"type": "parquet_source", "path": path}
        self.steps.append(step)
        self.listSteps.addItem(f"Parquet Source: {os.path.basename(path)}")

    def add_postgis_source(self):
        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("PostGIS source")
        form = QtWidgets.QFormLayout(dlg)
        le_conn = QtWidgets.QLineEdit("postgresql://user:password@localhost:5432/dbname")
        le_sql = QtWidgets.QLineEdit("SELECT * FROM mytable")
        form.addRow("SQLAlchemy URL", le_conn)
        form.addRow("SQL query", le_sql)
        btn = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        form.addRow(btn)
        btn.accepted.connect(dlg.accept)
        btn.rejected.connect(dlg.reject)
        if dlg.exec_() != QtWidgets.QDialog.Accepted:
            return
        step = {"type": "postgis_source", "url": le_conn.text(), "sql": le_sql.text()}
        self.steps.append(step)
        self.listSteps.addItem("PostGIS Source: " + le_sql.text()[:30])

    def add_source(self):
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "CSV source", "", "CSV Files (*.csv)")
        if not path:
            return
        step = {"type": "csv_source", "path": path}
        self.steps.append(step)
        self.listSteps.addItem(f"CSV Source: {os.path.basename(path)}")

    def add_transform(self):
        if not self.steps:
            QtWidgets.QMessageBox.warning(self, "Transform", "Add a source first.")
            return
        choice, ok = QtWidgets.QInputDialog.getItem(self, "Transform Type", "Select transform:", ["Filter", "Reproject", "Join"], 0, False)
        if not ok:
            return
        if choice == "Filter":
            expr, ok2 = QtWidgets.QInputDialog.getText(self, "Filter expression", "pandas/geopandas query expression")
            if not ok2:
                return
            step = {"type": "filter", "expr": expr}
            self.steps.append(step)
            self.listSteps.addItem(f"Filter: {expr}")
        elif choice == "Reproject":
            epsg, ok2 = QtWidgets.QInputDialog.getInt(self, "Reproject", "Target EPSG code", 4326, 1000, 999999)
            if not ok2:
                return
            step = {"type": "reproject", "epsg": epsg}
            self.steps.append(step)
            self.listSteps.addItem(f"Reproject → EPSG:{epsg}")
        elif choice == "Join":
            path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Lookup table (CSV/Parquet)", "", "Data (*.csv *.parquet *.pq)")
            if not path:
                return
            key_left, ok2 = QtWidgets.QInputDialog.getText(self, "Join", "Field in main table:")
            if not ok2:
                return
            key_right, ok3 = QtWidgets.QInputDialog.getText(self, "Join", "Field in lookup table:")
            if not ok3:
                return
            step = {"type": "join", "path": path, "left": key_left, "right": key_right}
            self.steps.append(step)
            self.listSteps.addItem(f"Join on {key_left}={key_right}")

    def add_filter(self):
        return self.add_transform()
        if not self.steps:
            QtWidgets.QMessageBox.warning(self, "Filter", "Add a source first.")
            return
        expr, ok = QtWidgets.QInputDialog.getText(self, "Filter expression", "pandas query expression, e.g. area > 1000")
        if not ok:
            return
        step = {"type": "filter", "expr": expr}
        self.steps.append(step)
        self.listSteps.addItem(f"Filter: {expr}")

    def add_cloud_source(self):
        url, ok = QtWidgets.QInputDialog.getText(self, "Cloud URL", "s3://bucket/file.parquet or gs://…")
        if not ok or not url:
            return
        step = {"type": "cloud_source", "url": url}
        self.steps.append(step)
        self.listSteps.addItem("Cloud Source: " + url[:40])

    def add_cloud_sink(self):
        url, ok = QtWidgets.QInputDialog.getText(self, "Cloud URL", "s3://bucket/output.parquet")
        if not ok or not url:
            return
        step = {"type": "cloud_sink", "url": url}
        self.steps.append(step)
        self.listSteps.addItem("Cloud Sink: " + url[:40])

    def add_parquet_sink(self):
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Parquet sink", "output.parquet", "Parquet Files (*.parquet *.pq)")
        if not path:
            return
        step = {"type": "parquet_sink", "path": path}
        self.steps.append(step)
        self.listSteps.addItem(f"Parquet Sink: {os.path.basename(path)}")

    def add_postgis_sink(self):
        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("PostGIS sink")
        form = QtWidgets.QFormLayout(dlg)
        le_conn = QtWidgets.QLineEdit("postgresql://user:password@localhost:5432/dbname")
        le_table = QtWidgets.QLineEdit("schema.table")
        form.addRow("SQLAlchemy URL", le_conn)
        form.addRow("Target table", le_table)
        btn = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        form.addRow(btn)
        btn.accepted.connect(dlg.accept)
        btn.rejected.connect(dlg.reject)
        if dlg.exec_() != QtWidgets.QDialog.Accepted:
            return
        step = {"type": "postgis_sink", "url": le_conn.text(), "table": le_table.text()}
        self.steps.append(step)
        self.listSteps.addItem("PostGIS Sink: " + le_table.text())

    def add_sink(self):
        if not self.steps:
            QtWidgets.QMessageBox.warning(self, "Sink", "Add steps first.")
            return
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "CSV sink", "output.csv", "CSV Files (*.csv)")
        if not path:
            return
        step = {"type": "csv_sink", "path": path}
        self.steps.append(step)
        self.listSteps.addItem(f"CSV Sink: {os.path.basename(path)}")

    def load_params_for_row(self, row: int):
        self.stackParams.setCurrentIndex(0)  # blank page for MVP

    def run_workflow(self):
        if not self.steps or self.steps[0]["type"] != "csv_source" or self.steps[-1]["type"] != "csv_sink":
            QtWidgets.QMessageBox.warning(self, "Run", "Pipeline must start with CSV Source and end with CSV Sink.")
            return

        task = _PipelineTask(self.steps)
        task.finished.connect(lambda ok: QtWidgets.QMessageBox.information(self, "Run", "Completed" if ok else "Failed"))
        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
        QtCore.QTimer.singleShot(50, lambda: QtWidgets.QApplication.restoreOverrideCursor())
        QtWidgets.QMessageBox.information(self, "Run", "Task submitted – check QGIS task manager for progress.")
        from qgis.core import QgsApplication
        QgsApplication.taskManager().addTask(task)

    def load_workflow(self):
        filename, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Open workflow", "", "JSON (*.json)")
        if not filename:
            return
        try:
            with open(filename, "r", encoding="utf-8") as fh:
                self.steps = json.load(fh)
            self.listSteps.clear()
            for step in self.steps:
                if step["type"] == "csv_source":
                    self.listSteps.addItem(f"CSV Source: {os.path.basename(step['path'])}")
                elif step["type"] == "filter":
                    self.listSteps.addItem(f"Filter: {step['expr']}")
                elif step["type"] == "csv_sink":
                    self.listSteps.addItem(f"CSV Sink: {os.path.basename(step['path'])}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Open", str(e))

    def save_workflow(self):
        filename, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Save workflow", "workflow.json", "JSON (*.json)")
        if not filename:
            return
        try:
            with open(filename, "w", encoding="utf-8") as fh:
                json.dump(self.steps, fh, indent=2)
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Save", str(e))


class _PipelineTask(QgsTask):
    def __init__(self, steps):
        super().__init__("ETL Pipeline")
        self.steps = steps

    def run(self):
        try:
            df = None
            for step in self.steps:
                if step["type"] == "csv_source":
                    df = pd.read_csv(step["path"])
                elif step["type"] == "filter":
                    df = df.query(step["expr"])
                elif step["type"] == "parquet_source":
                    df = gpd.read_parquet(step["path"]) if gpd else pd.read_parquet(step["path"])  # type: ignore
                elif step["type"] == "postgis_source":
                    engine = create_engine(step["url"])
                    df = gpd.read_postgis(step["sql"], engine)
                elif step["type"] == "reproject":
                    if hasattr(df, "to_crs"):
                        df = df.to_crs(epsg=step["epsg"])
                elif step["type"] == "join":
                    lookup = pd.read_parquet(step["path"]) if step["path"].endswith((".parquet", ".pq")) else pd.read_csv(step["path"])  # noqa
                    df = df.merge(lookup, left_on=step["left"], right_on=step["right"], how="left")
                elif step["type"] == "csv_sink":
                    df.to_csv(step["path"], index=False)
                elif step["type"] == "cloud_source":
                    storage_opts = {}
                    df = gpd.read_parquet(step["url"], storage_options=storage_opts) if gpd else pd.read_parquet(step["url"], storage_options=storage_opts)  # type: ignore
                elif step["type"] == "cloud_sink":
                    storage_opts = {}
                    if hasattr(df, 'to_parquet'):
                        df.to_parquet(step["url"], storage_options=storage_opts, index=False)
                    else:
                        pd.DataFrame(df).to_parquet(step["url"], storage_options=storage_opts, index=False)
                elif step["type"] == "parquet_sink":
                    if hasattr(df, 'to_parquet'):
                        df.to_parquet(step['path'], index=False)
                    else:
                        pd.DataFrame(df).to_parquet(step['path'], index=False)
                elif step["type"] == "postgis_sink":
                    engine = create_engine(step['url'])
                    df.to_postgis(step['table'], engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            self.setError(str(e))
            return False


