# -*- coding: utf-8 -*-
"""
Test classes for parsing protein interaction files.

Author:  Greg Taylor (greg.k.taylor@gmail.com)
"""
import os
import unittest

from .bitest import BITest
from hub.dataload.sources.hint.parser import HiNTParser


class TestParserMethods(BITest):
    """
    Test class for HiNT parser functions.  The static methods are called on an actual
    dataset.
    """

    hintFile = os.path.join(os.path.dirname(__file__), 'test_data/HomoSapiens_htb_hq.txt')

    def test_hint_parse(self):
        """
        Parse a test hint file, gather statistics, and assess results
        :return:
        """
        # Write the contents of the test ConsenesusPathDB file to a temporary file object
        test_file = open(TestParserMethods.hintFile, mode="r")
        hint = []
        for record in HiNTParser.parse_tsv_file(test_file):
            hint.append(record)

        ########################################################
        # Gather some useful statistics of the resulting dataset
        ########################################################

        self.assertGreater(self._list_average(hint, 'hint', 'evidence'), 2)

