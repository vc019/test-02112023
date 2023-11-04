from app.etl_config import EtlConfig


class TestETLConfig:
    """
    Test class for testing etl config
    """

    def test_read(self):
        expected_etl_map = [
            {
                "sql": "select a.legal_entity, a.counter_party, b.tier, a.rating, a.status, a.value from input_df1 a, input_df2 b where  a.counter_party = b.counter_party",
                "viewName": "base_df",
            },
            {
                "sql": 'select *, case when status = "ARAP" then value else 0 end arap_value, case when status = "ACCR" then value else 0 end accr_value from base_df',
                "viewName": "vals_df",
            },
            {
                "sql": "select counter_party, max(rating) max_counter_party_rating from base_df group by counter_party",
                "viewName": "rating_df",
            },
            {
                "sql": "select  legal_entity, counter_party, tier, sum(arap_value) sum_arap_value, sum(accr_value) sum_accr_value from vals_df group by legal_entity, counter_party, tier",
                "viewName": "aggs_df",
            },
            {
                "sql": "select a.legal_entity, a.counter_party, a.tier, b.max_counter_party_rating, sum_arap_value, sum_accr_value from aggs_df a, rating_df b where a.counter_party = b.counter_party",
                "viewName": "output_df",
            },
        ]
        etl_config = EtlConfig("etl_config.json")
        actual_etl_map: list[dict[str, str]] = etl_config.read()
        assert actual_etl_map == expected_etl_map

    def test_read_input(self):
        expected_input_config = [
            {"filePath": "dataset1.csv", "viewName": "input_df1"},
            {"filePath": "dataset2.csv", "viewName": "input_df2"},
        ]
        etl_config = EtlConfig("etl_config.json")
        actual_input_config = etl_config.read_input()
        assert actual_input_config == expected_input_config

    def test_read_output(self):
        expected_output_config = {
            "outputFilePath": "output.csv",
            "outputFileHeader": "legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)\n",
        }
        etl_config = EtlConfig("etl_config.json")
        actual_output_config = etl_config.read_output()
        assert actual_output_config == expected_output_config
