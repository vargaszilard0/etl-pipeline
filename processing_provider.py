"""Processing provider to run saved ETL workflows headlessly."""
from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (
    QgsProcessingProvider,
    QgsProcessingAlgorithm,
    QgsProcessingParameterFile,
    QgsProcessingException,
    QgsProcessing,
)
import json
from .etl_builder_panel import _PipelineTask


class ETLProcessingAlgorithm(QgsProcessingAlgorithm):
    INPUT = "INPUT"

    def createInstance(self):
        return ETLProcessingAlgorithm()

    def name(self):
        return "run_workflow"

    def displayName(self):
        return self.tr("Run ETL Workflow")

    def group(self):
        return self.tr("ETL")

    def groupId(self):
        return "etl"

    def shortHelpString(self):
        return self.tr("Executes a saved ETL workflow JSON file synchronously.")

    def initAlgorithm(self, cfg=None):
        self.addParameter(QgsProcessingParameterFile(self.INPUT, self.tr("Workflow JSON"), extension="json"))

    def processAlgorithm(self, params, context, feedback):
        path = self.parameterAsFile(params, self.INPUT, context)
        if not path:
            raise QgsProcessingException(self.tr("No input workflow provided"))
        try:
            feedback.pushInfo(f"Loading workflow from {path}")
            with open(path, "r", encoding="utf-8") as fh:
                steps = json.load(fh)
            task = _PipelineTask(steps)
            ok = task.run()
            if not ok:
                raise QgsProcessingException(task.error())
            feedback.pushInfo(self.tr("Workflow finished successfully"))
            return {}
        except Exception as e:
            raise QgsProcessingException(str(e))


class ETLProcessingProvider(QgsProcessingProvider):
    def loadAlgorithms(self):
        self.addAlgorithm(ETLProcessingAlgorithm())

    def id(self):
        return "etl"

    def name(self):
        return self.tr("ETL Pipeline Builder")

    def longName(self):
        return self.name()

    def tr(self, msg):
        return QCoreApplication.translate("ETLProcessingProvider", msg)
