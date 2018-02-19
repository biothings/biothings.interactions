# -*- coding: utf-8 -*-
"""
Test classes for parsing protein interaction files.

Author:  Greg Taylor (greg.k.taylor@gmail.com)
"""
import os
import unittest

from .bitest import BITest
from hub.dataload.sources.ndex.parser import nDEXParser


class TestParserMethods(BITest):
    """
    Test class for nDEX parser functions.  The static methods are called on an actual
    dataset.
    """

    ndexFile = os.path.join(os.path.dirname(__file__), 'test_data/nDEX.json')

    def test_ndex_parse(self):
        """
        Parse a test ndex file, gather statistics, and assess results
        :return:
        """
        # Write the contents of the test ConsenesusPathDB file to a temporary file object
        test_file = open(TestParserMethods.ndexFile, mode="r")
        ndex = []
        for record in nDEXParser.parse_ndex_file(test_file):
            ndex.append(record)

        ########################################################
        # Gather some useful statistics of the resulting dataset
        ########################################################

        # Rudimentary check on the number of interactions
        self.assertGreater(len(ndex), 6500)
