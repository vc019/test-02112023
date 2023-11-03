from app.etl_config import EtlConfig
from app.spark_etl import SparkETL


class TestSparkETL:
    def test_read_csv(self, spark_session):
        # Expected result
        expected_rows = 18

        # Test setup
        etl_config = EtlConfig("etl_config.json")
        etl_map: list[dict[str, str]] = etl_config.read()
        spark_etl = SparkETL(spark_session, etl_map)

        # Actual test
        spark_etl._SparkETL__read_csv("dataset1.csv", "input_df")
        actual_rows = len(
            spark_etl.spark_session.sql("select * from input_df").collect()
        )
        assert actual_rows == expected_rows

    def test_process_files(self, spark_session):
        # Expected result
        expected_rows_input_df1 = 18
        expected_rows_input_df2 = 6

        # Test setup
        etl_config = EtlConfig("etl_config.json")
        etl_map: list[dict[str, str]] = etl_config.read()
        input_dataset: list[dict[str, str]] = etl_config.read_input()
        spark_etl = SparkETL(spark_session, etl_map)

        # Actual test
        spark_etl._SparkETL__read_input_datasets(input_dataset)
        actual_rows_input_df1 = len(
            spark_etl.spark_session.sql("select * from input_df1").collect()
        )
        actual_rows_input_df2 = len(
            spark_etl.spark_session.sql("select * from input_df2").collect()
        )
        assert actual_rows_input_df1 == expected_rows_input_df1
        assert actual_rows_input_df2 == expected_rows_input_df2

    def test_compute_aggregates(self, spark_session):
        # Expected result
        expected_rows = 8

        # Test setup
        etl_config = EtlConfig("etl_config.json")
        etl_map: list[dict[str, str]] = etl_config.read()
        input_dataset: list[dict[str, str]] = etl_config.read_input()
        spark_etl = SparkETL(spark_session, etl_map)
        spark_etl._SparkETL__read_input_datasets(input_dataset)

        # Actual test
        spark_etl._SparkETL__compute_aggregates()
        actual_output_rows = len(
            spark_etl.spark_session.sql("select * from output_df").collect()
        )
        assert actual_output_rows == expected_rows

    def test_process_files2(self, spark_session):
        # Expected result
        expected_rows = 8

        # Test setup
        etl_config = EtlConfig("etl_config.json")
        etl_map: list[dict[str, str]] = etl_config.read()
        input_dataset: list[dict[str, str]] = etl_config.read_input()
        spark_etl = SparkETL(spark_session, etl_map)

        # Actual test
        spark_etl.process_files(input_dataset)
        actual_output_rows = len(
            spark_etl.spark_session.sql("select * from output_df").collect()
        )
        assert actual_output_rows == expected_rows

    def test_get_output(self, spark_session):
        # Expected result
        expected_rows = 8

        # Test setup
        etl_config = EtlConfig("etl_config.json")
        etl_map: list[dict[str, str]] = etl_config.read()
        input_dataset: list[dict[str, str]] = etl_config.read_input()
        spark_etl = SparkETL(spark_session, etl_map)
        spark_etl.process_files(input_dataset)

        # Actual test
        output_data = spark_etl._SparkETL__get_output()
        actual_output_rows = len(output_data)
        assert actual_output_rows == expected_rows

    def test_write_csv(self, spark_session):
        # Expected result
        expected_rows = 9

        # Test setup
        etl_config = EtlConfig("etl_config.json")
        etl_map: list[dict[str, str]] = etl_config.read()
        input_dataset: list[dict[str, str]] = etl_config.read_input()
        output_config: dict[str, str] = etl_config.read_output()

        spark_etl = SparkETL(spark_session, etl_map)
        spark_etl.process_files(input_dataset)
        spark_etl.write_csv(
            output_file_path=output_config.get("outputFilePath"),
            output_header=output_config.get("outputFileHeader"),
        )

        actual_rows = sum(1 for _ in open(output_config.get("outputFilePath")))
        assert actual_rows == expected_rows
