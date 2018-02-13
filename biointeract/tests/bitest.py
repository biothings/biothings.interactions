# -*- coding: utf-8 -*-
"""
Test class for parsing protein interaction files.

Author:  Greg Taylor (greg.k.taylor@gmail.com)
"""
import unittest


class BITest(unittest.TestCase):
    """
    Test class for generic parser functions.
    """

    def _num_values(self, records, evidence, field):
        """
        Compute the total number of non NoneType values for a field in a given record
        in the evidence dictionaries.
        :param records:
        :param field:
        :return:
        """
        # Number of records with non-null values
        total = 0
        for _r in records:
            for _f in _r[evidence]:
                if field in _f.keys():
                    total = total + 1
        return total

    def _total(self, records, evidence, field):
        """
        Compute the sum total over the test dataset for a given record field
        in the evidence dictionaries.
        :param records:
        :param field:
        :return:
        """
        # Number of records with non-null values
        total = 0
        for _r in records:
            for _f in _r[evidence]:
                if _f[field]:
                    total = total + _f[field]
        return total

    def _list_count(self, records, evidence, field):
        """
        Compute the count of list elements over the test dataset for a given record field
        in the evidence dictionaries.
        :param records:
        :param field:
        :return:
        """
        count = 0
        for _r in records:
            for _f in _r[evidence]:
                if field in _f:
                    count = count + len(_f[field])
        return count

    def _list_average(self, cpd, evidence, field):
        """
        Compute the average number of list elements over the test dataset for a given record field.
        :param cpd:
        :param field:
        :return:
        """
        count = self._list_count(cpd, evidence, field)
        return count / len(cpd)

    def _record_average(self, records, field1, field2):
        """
        Compute the average number of list elements over the test dataset for a given record field.
        :param cpd:
        :param field:
        :return:
        """
        count = 0
        for _r in records:
            if field1 in _r.keys() and field2 in _r[field1].keys():
                count = count + len(_r[field1][field2])
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
