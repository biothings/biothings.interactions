# -*- coding: utf-8 -*-
"""
Test classes for parsing protein interaction files.

Author:  Greg Taylor (greg.k.taylor@gmail.com)
"""
import os
import unittest

from .bitest import BITest
from hub.dataload.sources.disgenet.parser import DisGeNETParser


class TestParserMethods(BITest):
    """
    Test class for DisGeNET parser functions.
    """

    disgenetFile = os.path.join(os.path.dirname(__file__), 'test_data/curated_gene_disease_associations.tsv')

    def test_disgenet_parse(self):
        """
        Parse a test DisGeNET file, gather statistics, and assess results
        :return:
        """
        # Write the contents of the test ConsenesusPathDB file to a temporary file object
        test_file = open(TestParserMethods.disgenetFile, mode="r")
        disgenet = []
        for record in DisGeNETParser.parse_tsv_file(test_file):
            disgenet.append(record)

        ########################################################
        # Gather some useful statistics of the resulting dataset
        ########################################################
        self.assertGreater(self._average(disgenet, 'disgenet', 'nofpmids'), 0.3)
        self.assertGreater(self._average(disgenet, 'disgenet', 'score'), 0.2)
        self.assertGreater(self._average(disgenet, 'disgenet', 'nofsnps'), 0.3)
