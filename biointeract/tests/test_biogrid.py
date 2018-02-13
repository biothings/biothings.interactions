# -*- coding: utf-8 -*-
"""
Test classes for parsing protein interaction files.

Author:  Greg Taylor (greg.k.taylor@gmail.com)
"""
import os
import unittest

from .bitest import BITest
from hub.dataload.sources.biogrid.parser import BiogridParser


class TestParserMethods(BITest):
    """
    Test class for BioGRID parser functions.  The static methods are called on the actual
    dataset.
    """

    biogridFile = os.path.join(os.path.dirname(__file__), 'test_data/BIOGRID-ALL-3.4.156.tab2.txt')

    def test_biogrid_parse(self):
        """
        Parse a test biogrid file of 1000 lines, gather statistics, and assess results
        :return:
        """
        # Write the contents of the test ConsenesusPathDB file to a temporary file object
        test_file = open(TestParserMethods.biogridFile, mode="r")
        biogrid = []
        for record in BiogridParser.parse_biogrid_tsv_file(test_file):
            biogrid.append(record)

        ########################################################
        # Gather some useful statistics of the resulting dataset
        ########################################################

        self.assertGreater(self._num_values(biogrid, 'biogrid', 'score'), 800000)
        self.assertGreater(self._num_values(biogrid, 'biogrid', 'modification'), 19000)
        self.assertGreater(self._list_average(biogrid, 'biogrid', 'phenotypes'), 0.4)
        self.assertGreater(self._list_average(biogrid, 'biogrid', 'qualifications'), 1.1)
        self.assertEqual(self._num_values(biogrid, 'biogrid', 'tags'), 0)

        # Average number of Synonyms for Interactor A
        self.assertGreater(self._record_average(biogrid, 'interactor_a', 'synonyms'), 2.4)
        # Average number of Synonyms for Interactor B
        self.assertGreater(self._record_average(biogrid, 'interactor_b', 'synonyms'), 2.3)
