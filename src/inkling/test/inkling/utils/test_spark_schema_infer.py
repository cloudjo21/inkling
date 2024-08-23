import unittest

from inkling.utils.spark_schema_infer import SparkDataFrameSchemaInferer


class SparkSchemaInferTest(unittest.TestCase):

    def setUp(self):
        self.schema_inferer = SparkDataFrameSchemaInferer()

    def test_single_schema_or_field(self):
        schema = self.schema_inferer.get_schema('charge_for_play', '13000')
        assert schema is not None

        schema = self.schema_inferer.get_schema('care_intro_len', 296.0)
        assert schema is not None

        schema = self.schema_inferer.get_schema('accept_tag_ct', {"놀이터": 3, "동요부르기": 2, "동화구연": 1, "실내놀이": 3, "책읽기": 2, "견학_체험": 1, "보드게임": 1})
        assert schema is not None

        schema = self.schema_inferer.get_schema('accept_tag_ct', '{"놀이터": 3, "동요부르기": 2, "동화구연": 1, "실내놀이": 3, "책읽기": 2, "견학_체험": 1, "보드게임": 1}')
        print(schema)
        assert schema is not None

        schema = self.schema_inferer.get_schema('accept_tag_ct', ["놀이터", "동요부르기", "동화구연"])
        assert schema is not None

        schema = self.schema_inferer.get_schema('accept_tag_ct', [0.5, 0.1, 0.4])
        assert schema is not None

        schema = self.schema_inferer.get_schema('PassionAvg', float('nan'), return_field=True)
        assert schema is not None
