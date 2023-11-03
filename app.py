import sys
from pyspark.sql import SparkSession

from app.etl_config import EtlConfig
from app.spark_etl import SparkETL


def create_context(context_type: str, app_name: str) -> SparkSession:
    """
    Returns the spark session based on configuration.
    :param context_type:
    :param app_name:
    :return: spark_session
    """
    match context_type:
        case "spark_local":
            return SparkSession.builder \
                .appName(app_name) \
                .master("local[2]") \
                .getOrCreate()
        case "spark_cluster":
            return SparkSession.builder \
                .appName(app_name) \
                .master() \
                .getOrCreate()
        case _:
            return None


if __name__ == "__main__":
    # Read ETL config
    etl_config = EtlConfig("etl_config.json")
    input_dataset: list[dict[str, str]] = etl_config.read_input()
    etl_map: list[dict[str, str]] = etl_config.read()
    output_config: dict[str, str] = etl_config.read_output()

    # Create a spark session
    spark_session: SparkSession = create_context(context_type=sys.argv[1], app_name="sparkETLApp")
    if spark_session is None:
        print("Unable to initialise Spark")
        sys.exit(100)

    # Initialise the ETL
    spark_etl = SparkETL(spark_session, etl_map)

    # Run Spark ETL
    spark_etl.process_files(input_dataset)

    # Create CSV file
    spark_etl.write_csv(output_file_path=output_config.get("outputFilePath"),
                        output_header=output_config.get("outputFileHeader"))
