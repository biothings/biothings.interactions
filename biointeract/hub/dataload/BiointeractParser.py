"""
BiogridParser parses the biogrid data file and yields
a generated dictionary of record values.

For a description of the BIOGRID file format see the
following link:

https://wiki.thebiogrid.org/doku.php/biogrid_tab_version_2.0

Source Project:   biothings.interactions
Author:  Greg Taylor:  greg.k.taylor@gmail.com
"""
import re


class BiointeractParser(object):

    @staticmethod
    def parse_list(entry, separator):
        """
        Parse all list entries given as string from the tsv file.
        The resulting strings will be returned,
        :param entry: a string representing the list
        :param separator: the deliminator for the list
        :return: list of strings
        """
        return [x for x in entry.split(separator)] if entry else None

    @staticmethod
    def group_fields(r, group_name, fields):
        """
        Given a dictionary r, group all fields 'fields' together into a dictionary
        labeled by a 'group_name' in r.
        :param r:  The dictionary record
        :param group_name: the group name to group fields under
        :param fields:  fields to group together
        :return:  a new dictionary record r
        """
        g = {}
        for f in fields.keys():
            g[fields[f]] = r.pop(f, None)
        r[group_name] = g
        return r

    @staticmethod
    def parse_int_fields(record, int_fields):
        """
        Parse integer fields in a biogrid record dictionary
        :param entry: a record dictionary
        :return: a converted record dictionary
        """
        for field in int_fields:
            record[field] = BiointeractParser.safe_int(record[field])
        return record

    @staticmethod
    def safe_int(str):
        """
        Utility function to convert a string to an integer returning 0 if the
        conversion of unsuccessful.
        :param str:
        :return:
        """
        if not str:
            return None
        try:
            return int(str)
        except ValueError:
            return 0

    @staticmethod
    def safe_float(str):
        """
        Utility function to convert a string to a float returning 0 if the
        conversion of unsuccessful.
        :param str:
        :return:
        """
        if not str:
            return None
        try:
            return float(str)
        except ValueError:
            return 0
