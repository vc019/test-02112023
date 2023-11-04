import json


class EtlConfig:
    """
    Class to read etl configuration file
    """

    def __init__(self, file_path) -> None:
        self.file_path = file_path
        self.config = None
        self.etl_map = None
        self.input_config = None
        self.output_config = None

        # Read the configuration
        self.__read_config()

    def __read_config(self) -> None:
        """
        Read the configuration
        :return:
        """
        if self.config is None:
            with open(self.file_path, "r") as jf:
                self.config = json.load(jf)

    def read(self) -> list[dict[str, str]]:
        """
        Reads etl configuration
        :return: Returns a list of dictionaries containing steps to execute
        """
        if self.etl_map is None:
            self.etl_map = self.config["etlMap"]

        return self.etl_map

    def read_input(self) -> list[dict[str, str]]:
        """
        Read the input dataset configuration
        :return:
        """
        if self.input_config is None:
            self.input_config = self.config["inputDataSet"]

        return self.input_config

    def read_output(self) -> dict[str, str]:
        """
        Reads output configuration
        :return:
        """
        if self.output_config is None:
            self.output_config = self.config["outputConfig"]

        return self.output_config
