# -*- coding: utf-8 -*-
"""
Test classes for parsing protein interaction files.

Author:  Greg Taylor (greg.k.taylor@gmail.com)
"""
import os
import unittest
import tempfile

from hub.dataload.sources.ConsensusPathDB.parser import CPDParser
from hub.dataload.sources.biogrid.parser import BiogridParser


class TestParserMethods(unittest.TestCase):
    """
    Test class for ConsensusPathDB parser functions.  The static methods are called on a representative
    dataset.

    The datasets were extracted from a debugging screen query, results were pruned down to one entry
    and results were manually validated.
    """

    # ConsensusPathDBFile = os.path.join(os.path.dirname(__file__), 'test_data/randomized-dataset-001')
    # biogridFile = os.path.join(os.path.dirname(__file__), 'test_data/randomized-dataset-002')

    ConsensusPathDBFile = os.path.join(os.path.dirname(__file__), 'test_data/ConsensusPathDB_human_PPI')
    biogridFile = os.path.join(os.path.dirname(__file__), 'test_data/BIOGRID-ALL-3.4.154.tab2.txt')


    def test_CPD_parse(self):
        """
        Parse a test CPD file of 1000 lines, gather statistics, and assess results
        :return:
        """
        # Write the contents of the test ConsenesusPathDB file to a temporary file object
        test_file = open(TestParserMethods.ConsensusPathDBFile, mode="r")
        cpd = []
        for record in CPDParser.parse_cpd_tsv_file(test_file):
            cpd.append(record)

        ########################################################
        # Gather some useful statistics of the resulting dataset
        ########################################################
        # Sum total of interaction confidence
        self.assertGreater(self._total(cpd, 'interaction_confidence'), 305)

        # Average number of interact_participants
        self.assertGreater(self._list_average(cpd, 'interaction_participants'), 2.2)
        # Average number of interaction_publications
        self.assertGreater(self._list_average(cpd, 'interaction_publications'), 1.4)
        # Average number of source_databases
        self.assertGreater(self._list_average(cpd, 'source_databases'), 1.4)

        n = self._none_count(cpd)
        self.assertEqual(n['source_databases'], 0)
        self.assertLess(n['interaction_publications'], 2800)
        self.assertEqual(n['interaction_participants'], 0)
        self.assertLess(n['interaction_confidence'], 19000)

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

        self.assertGreater(self._num_values(biogrid, 'Score'), 800000)
        self.assertGreater(self._num_values(biogrid, 'Modification'), 19000)
        self.assertGreater(self._list_average(biogrid, 'Phenotypes'), 0.4)
        self.assertGreater(self._list_average(biogrid, 'Qualifications'), 1.1)
        self.assertEqual(self._num_values(biogrid, 'Tags'), 0)

        # Average number of Synonyms for Interactor A
        self.assertGreater(self._record_average(biogrid, 'Interactor A', 'Synonyms'), 2.5)
        # Average number of Synonyms for Interactor B
        self.assertGreater(self._record_average(biogrid, 'Interactor B', 'Synonyms'), 2.5)

        # Get the NoneType count of the record set
        n = self._none_count(biogrid)
        self.assertLess(n['Score'], 670000)
        self.assertLess(n['Modification'], 1500000)
        self.assertLess(n['Phenotypes'], 900000)
        self.assertLess(n['Qualifications'], 500000)
        self.assertLess(n['Tags'], 1500000)
        self.assertEqual(n['Interactor A']['BioGRID ID'], 0)
        self.assertLess(n['Interactor A']['Systematic Name'], 320000)
        self.assertEqual(n['Interactor A']['Official Symbol'], 0)
        self.assertLess(n['Interactor A']['Synonyms'], 360000)
        self.assertLess(n['Interactor B']['Systematic Name'], 320000)

    def _num_values(self, records, field):
        """
        Compute the total number of non NoneType values for a field in a given record.
        :param records:
        :param field:
        :return:
        """
        # Number of records with non-null values
        total = 0
        for _r in records:
            if _r[field]:
                total = total + 1
        return total

    def _total(self, cpd, field):
        """
        Compute the sum total over the test dataset for a given record field.
        :param cpd:
        :param field:
        :return:
        """
        # Number of records with non-null values
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

    def _record_average(self, records, field1, field2):
        """
        Compute the average number of list elements over the test dataset for a given record field.
        :param cpd:
        :param field:
        :return:
        """
        count = 0
        for record in records:
            if record[field1][field2]:
                count = count + len(record[field1][field2])
        return count / len(records)

    def _find20(self, records, field):
        """

        :param records:
        :param field:
        :return:
        """
        i = 0
        for r in records:
            if r[field]:
                print("%s:%s" % (field, r[field]))
                i = i + 1
            if i >= 20:
                break

    def _none_count(self, records):
        """
        Count the number of NoneType occurences within the record set.
        :param records:
        :return:
        """

        # Data structure to return
        r = {}
        for field in records[0].keys():
            if isinstance(records[0][field], dict):
                r[field] = {}
                for subfield in records[0][field].keys():
                    r[field][subfield] = 0
            else:
                r[field] = 0

        # Calculate all none types - special case for dictionaries
        for record in records:
            for field in record.keys():
                if isinstance(record[field], dict):
                    for subfield in record[field].keys():
                        if not record[field][subfield]:
                            r[field][subfield] = r[field][subfield] + 1
                elif not record[field]:
                    r[field] = r[field] + 1

        return r
