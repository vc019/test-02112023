import logging

from pyspark.sql import SparkSession, Row


class SparkETL:
    """
    Class for running Spark ETL
    """

    def __init__(
        self, spark_session: SparkSession, etl_map: list[dict[str, str]]
    ) -> None:
        self.spark_session: SparkSession = spark_session
        self.etl_map = etl_map
        self.output_rows: list[Row] = None
        self.logger = logging.getLogger("py4j")

    def process_files(self, dataset_input: list[dict[str, str]]) -> None:
        """
        Process datasets using spark etl
        :param dataset_input:
        :return:
        """
        # Read input dataset files
        self.__read_input_datasets(dataset_input=dataset_input)

        # Compute aggregates
        self.__compute_aggregates()

    def __read_input_datasets(self, dataset_input: list[dict[str, str]]) -> None:
        """
        Read all the input datasets and create corresponding spark views.
        :param dataset_input:
        :return:
        """
        # Read the files
        list(
            map(
                lambda el: self.__read_csv(el.get("filePath"), el.get("viewName")),
                dataset_input,
            )
        )

    def __read_csv(self, file_path: str, spark_view_name: str) -> None:
        """
        Read the csv file and create a spark view
        :param file_path:
        :return:
        """
        self.spark_session.read.option("header", "true").csv(
            file_path
        ).createOrReplaceTempView(spark_view_name)

    def __compute_aggregates(self) -> None:
        """
        Computes the aggregates by reading a list of dictionaries.
        :return:
        """
        # Dictionary containing a map of dataframes sql and spark views to be created
        list(
            map(
                lambda el: self.__register_spark_view(
                    el.get("sql"), el.get("viewName")
                ),
                self.etl_map,
            )
        )

    def __register_spark_view(self, sql, view_name) -> None:
        """
        Register a spark view based on spark sql
        :param sql: SQL for spark dataframe
        :param view_name: Name of spark view
        :return:
        """
        self.spark_session.sql(sql).createOrReplaceTempView(view_name)

    def __get_output(self) -> list[Row]:
        """
        Converts a spark dataframe into a list of Row objects
        :return: Returns a list of rows
        """
        if self.output_rows is None:
            self.output_rows = self.spark_session.sql(
                "select * from output_df"
            ).collect()
        return self.output_rows

    def write_csv(self, output_file_path, output_header) -> None:
        """
        Generates a csv file based on the path and header passed.
        :param output_header:
        :param output_file_path:
        :return: None
        """
        output_rows = self.__get_output()
        with open(output_file_path, "w") as fh:
            fh.write(output_header)
            for output_row in output_rows:
                output_str = ",".join(map(str, output_row))
                fh.write(f"{output_str} \n")
