'''Init for ETL Pipeline Builder plugin.'''

from .etl_pipeline_builder import ETLPipelineBuilder

def classFactory(iface):
    return ETLPipelineBuilder(iface)
