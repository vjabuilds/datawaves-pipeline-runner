import hydra
import findspark

findspark.init()
findspark.add_jars('postgresql-42.5.0.jar')

from datawaves_pipeline_runner.operators.core import PipelineOperator
from datawaves_pipeline_runner.operators.loaders.spark_loaders import \
    SparkTableLoader
from datawaves_pipeline_runner.operators.loggers import PrinterOperator
from datawaves_pipeline_runner.operators.transforms import NormalizeOperator
from datawaves_pipeline_runner.operators.writers import CsvWriter
from datawaves_pipeline_runner.util import PipelineManager, get_spark

@hydra.main(
    version_base=None,
    config_path="../datawaves_pipeline_runner/configs",
    config_name="config",
)
def run_pipeline(cfg):
    spark = get_spark("../configs", "config")
    props = {
        "user": "datawaves",
        "password": "pwd123",
        "driver": "org.postgresql.Driver",
    }

    flowers_dc = "flowers_dc"
    table_name = "flowers"
    url = "jdbc:postgresql://localhost:5432/datawaves"

    field_names = ["sepal_length", "sepal_width", "petal_length", "petal_width"]

    pipeline = PipelineOperator(
        "normalization",
        [
            SparkTableLoader("table_loader", flowers_dc, spark, table_name, url, props),
            PrinterOperator("loaded_printer", "Table has been loaded into spark!"),
            *[
                NormalizeOperator(f"normalize_{name}", flowers_dc, name)
                for name in field_names
            ],
            PrinterOperator(
                "normalized_fields", "Normalized all the fields in the flowers table!"
            ),
            CsvWriter("csv_writer", flowers_dc, "normalized"),
        ],
    )
    pipeline.run()
    pm = PipelineManager()
    pm.set_pipeline(pipeline)
    pm.save_pipeline('pipeline.yaml')


if __name__ == "__main__":
    run_pipeline()
