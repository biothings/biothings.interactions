# -*- coding: utf-8 -*-
"""
Test classes for parsing protein interaction files.

Author:  Greg Taylor (greg.k.taylor@gmail.com)
"""
import os
import unittest

from .bitest import BITest
from hub.dataload.sources.ctdchemgene.parser import CTDChemGeneParser


class TestParserMethods(BITest):
    """
    Test class for CTD parser functions.
    """

    ctdFile = os.path.join(os.path.dirname(__file__), 'test_data/CTD_chem_gene_ixns.tsv')

    def test_ctdchemgene_parse(self):
        """
        Parse a test hint file, gather statistics, and assess results
        :return:
        """
        # Write the contents of the test ConsenesusPathDB file to a temporary file object
        test_file = open(TestParserMethods.ctdFile, mode="r")
        ctd = []
        for record in CTDChemGeneParser.parse_tsv_file(test_file):
            ctd.append(record)

        ########################################################
        # Gather some useful statistics of the resulting dataset
        ########################################################

        self.assertGreater(self._list_average(ctd, 'ctd', 'pubmedids'), 1.5)
        self.assertGreater(self._list_average(ctd, 'ctd', 'interactionactions'), 2)
