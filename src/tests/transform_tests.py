# -*- coding: utf-8 -*-
import json
import unittest

from src.www.api.transform import ESResultTransformer


class TestStringMethods(unittest.TestCase):
    """
    Test class for the ESResultTransformer class.  The static methods are called on a representative
    dataset.  The four static methods are called and a basic smoke test is run on the results.

    The datasets were extracted from a debugging screen query, results were pruned down to one entry
    and results were manually validated.
    """

    test_result_json = """{"took":5,"timed_out":false,"_shards":{"total":10,"successful":10,"failed":0},"hits":{"total":3173,"max_score":1.0,"hits":[{"_index":"human_ppi_current","_type":"human_ppi","_id":"AV7tgtgwsEC71Kh3t5_-","_score":1.0,"_source":{"interaction_participants": "TRIO_HUMAN,BLMH_HUMAN", "source_databases": "PhosphoPOINT,Spike,IntAct,HPRD,MINT,Biogrid", "interaction_publications": "16169070", "interaction_confidence": "0.82"}}]}}"""
    test_hit_json = """{"_index":"human_ppi_current","_type":"human_ppi","_id":"AV7tgtgwsEC71Kh3t5_-","_score":1.0,"_source":{"interaction_participants": "TRIO_HUMAN,BLMH_HUMAN", "source_databases": "PhosphoPOINT,Spike,IntAct,HPRD,MINT,Biogrid", "interaction_publications": "16169070", "interaction_confidence": "0.82"}}"""
    i_hit = 0

    def test_parse_confidence(self):
        test_data = json.loads(self.test_result_json)
        test_hit = json.loads(self.test_hit_json)

        result_data = ESResultTransformer.parse_confidence(test_data, self.i_hit, test_hit)

        self.assertEqual(result_data['hits']['hits'][0]['_source'][ESResultTransformer.PpiFields.CONF], .82)

    def test_parse_participants(self):
        test_data = json.loads(self.test_result_json)
        test_hit = json.loads(self.test_hit_json)

        result_data = ESResultTransformer.parse_participants(test_data, self.i_hit, test_hit)

        self.assertEqual(len(result_data['hits']['hits'][0]['_source'][ESResultTransformer.PpiFields.PART]), 2)

    def test_parse_publications(self):
        test_data = json.loads(self.test_result_json)
        test_hit = json.loads(self.test_hit_json)

        result_data = ESResultTransformer.parse_publications(test_data, self.i_hit, test_hit)

        self.assertEqual(result_data['hits']['hits'][0]['_source'][ESResultTransformer.PpiFields.PUBS][0], 16169070)

    def test_parse_databases(self):
        test_data = json.loads(self.test_result_json)
        test_hit = json.loads(self.test_hit_json)

        result_data = ESResultTransformer.parse_databases(test_data, self.i_hit, test_hit)

        self.assertEqual(len(result_data['hits']['hits'][0]['_source'][ESResultTransformer.PpiFields.DBS]), 6)


if __name__ == '__main__':
    unittest.main()