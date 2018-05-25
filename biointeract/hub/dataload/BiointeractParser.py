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
from biothings.utils.dataload import dict_sweep


class BiointeractParser(object):

    @staticmethod
    def parse_list(entry, separator):
        """
        Parse all list entries given as string from the tsv file.
        The resulting strings will be returned,
        :param entry: a string representing the list
        :param separator: the deliminator for the list
        :return: list of strings, single element, or None
        """
        r = [x for x in entry.split(separator)] if entry else None

        # for lists with only a single element, return just the element
        if isinstance(r, list) and len(r) == 1:
            return r[0]
        else:
            return r

    @staticmethod
    def parse_int_list(entry, separator):
        """
        Parse a string entry into a list of integers.
        :param entry: a string representing the list
        :param separator: the deliminator for the list
        :return: list of integers, single integer, or None
        """
        r = BiointeractParser.parse_list(entry, separator)
        if isinstance(r, list):
            r = [BiointeractParser.safe_int(x) for x in r]
        else:
            r = BiointeractParser.safe_int(r)
        return r

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
    def rename_fields(r, rename_map):
        """
        Rename all fields to follow the biothings convention using lowercases and
        underscores.  Further, rename fields using the parameter 'rename_map'.
        :param r:
        :param rename_map:
        :return:
        """
        new_record = {}
        for f in r.keys():
            if f in rename_map.keys():
                new_record[rename_map[f]] = r[f]
            else:
                new_key = f.lower().replace(' ', '_')
                new_record[new_key] = r[f]
        return new_record

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

    @staticmethod
    def sweep_record(r):
        """
        Remove all None fields from a record
        :param r:
        :return:
        """
        return dict_sweep(r, vals=[None])

    @staticmethod
    def collapse_duplicate_keys(result_list, db_field):
        """
        Collapse duplicate keys
        :param result_list:
        :return:
        """
        # Add the id and record to the cache
        cache = {}
        print(len(result_list))

        for r in result_list:
            id = r['_id']
            if id not in cache.keys():
                cache[id] = r
            else:
                cache[id][db_field] = cache[id][db_field] + r[db_field]

        # transforms the cache back to a list
        pruned_result = []
        for k in cache.keys():
            pruned_result.append(cache[k])

        return pruned_result

    @staticmethod
    def extract_interactors(result_list, db_field):
        """
        Pull out interactor_a / interactor_b
        :param result_list:
        :return:
        """
        mod_result = []
        for k in result_list:
            r = {}
            r['interactor_a'] = k['interactor_a']
            k.pop('interactor_a')
            r['interactor_b'] = k['interactor_b']
            k.pop('interactor_b')
            if '_id' in k:
                r['_id'] = k['_id']
                k.pop('_id')
            r[db_field] = [k]
            mod_result.append(r)
        return mod_result
