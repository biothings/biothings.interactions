# -*- coding: utf-8 -*-
"""
Test classes for parsing protein interaction files.

Author:  Greg Taylor (greg.k.taylor@gmail.com)
"""
import os
import unittest
import tempfile

from hub.dataload.sources.ConsensusPathDB.parser import CPDParser


class TestCPDParserMethods(unittest.TestCase):
    """
    Test class for ConsensusPathDB parser functions.  The static methods are called on a representative
    dataset.

    The datasets were extracted from a debugging screen query, results were pruned down to one entry
    and results were manually validated.
    """

    ConsensusPathDBFile = os.path.join(os.path.dirname(__file__), 'test_data/randomized-dataset-001')

    def test_CPD_parse(self):
        """
        Parse a test CPD file of 100 lines, gather statistics, and assess results
        :return:
        """
        # Write the contents of the test ConsenesusPathDB file to a temporary file object
        test_file = open(TestCPDParserMethods.ConsensusPathDBFile, mode="r")
        cpd = []
        for record in CPDParser.parse_cpd_tsv_file(test_file):
            cpd.append(record)

        ########################################################
        # Gather some useful statistics of the resulting dataset
        ########################################################
        # Sum total of interaction confidence
        self.assertGreater(self._total(cpd, 'interaction_confidence'), 305)

        # Average number of interact_participants
        self.assertGreater(self._list_average(cpd, 'interaction_participants'), 2.5)

        # Average number of interaction_publications
        self.assertGreater(self._list_average(cpd, 'interaction_publications'), 3.5)

        # Average number of source_databases
        self.assertGreater(self._list_average(cpd, 'source_databases'), 2.5)

    def _total(self, cpd, field):
        """
        Compute the sum total over the test dataset for a given record field.
        :param cpd:
        :param field:
        :return:
        """
        # Number of records with non-null confidence
        total = 0
        for _c in cpd:
            if _c[field]:
                total = total + _c[field]
        return total

    def _list_count(self, cpd, field):
        """
        Compute the count of list elements over the test dataset for a given record field.
        :param cpd:
        :param field:
        :return:
        """
        count = 0
        for _c in cpd:
            if _c[field]:
                count = count + len(_c[field])
        return count

    def _list_average(self, cpd, field):
        """
        Compute the average number of list elements over the test dataset for a given record field.
        :param cpd:
        :param field:
        :return:
        """
        count = self._list_count(cpd, field)
        return count / len(cpd)
