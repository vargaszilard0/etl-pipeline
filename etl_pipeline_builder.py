"""
ETL Pipeline Builder - main plugin class
"""
import os
from qgis.PyQt.QtCore import QCoreApplication
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction
from .etl_builder_panel import ETLBuilderPanel


from .processing_provider import ETLProcessingProvider

class ETLPipelineBuilder:
    def __init__(self, iface):
        self.iface = iface
        self.plugin_dir = os.path.dirname(__file__)
        self.menu = self.tr("&ETL Pipeline Builder")
        self.actions = []
        self.dock = None
        self.provider = ETLProcessingProvider()

    def tr(self, message: str) -> str:
        return QCoreApplication.translate("ETLPipelineBuilder", message)

    def add_action(self, icon_path, text, callback, parent=None):
        icon = QIcon(icon_path)
        action = QAction(icon, text, parent)
        action.triggered.connect(callback)
        self.iface.addPluginToMenu(self.menu, action)
        self.iface.addToolBarIcon(action)
        self.actions.append(action)
        return action

    def initGui(self):
        icon = ":/images/themes/default/mActionAddLayer.svg"
        self.add_action(icon, self.tr("ETL Pipeline Builder"), self.open_panel, parent=self.iface.mainWindow())
        from qgis import processing
        processing.providerRegistry().addProvider(self.provider)

    def unload(self):
        for a in self.actions:
            self.iface.removePluginMenu(self.menu, a)
            self.iface.removeToolBarIcon(a)
        if self.dock is not None:
            self.iface.removeDockWidget(self.dock)
        from qgis import processing
        processing.providerRegistry().removeProvider(self.provider)

    def open_panel(self):
        if self.dock is None:
            self.dock = ETLBuilderPanel(self.iface.mainWindow())
            self.iface.addDockWidget(0x1, self.dock)  # Left dock area
        self.dock.show()
        self.dock.raise_()
