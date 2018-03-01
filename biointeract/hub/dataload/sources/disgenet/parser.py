"""
DisGeNETParser parses the DisGeNET data file and yields
a generated dictionary of record values.

Source Project:   biothings.interactions
Author:  Greg Taylor:  greg.k.taylor@gmail.com
"""
import json
import logging
import operator
import re

from hub.dataload.BiointeractParser import BiointeractParser
from biothings.utils.dataload import dict_sweep

class DisGeNETParser(BiointeractParser):
    # Static Constants
    EMPTY_FIELD = 'n/a'
    SEPARATOR = '|'

    # all default field names are ok
    rename_map = {
    }

    int_fields = [
        'nofpmids',
        'nofsnps'
    ]

    ###############################################################
    # Fields to be grouped into single documents within each record
    ###############################################################
    interactor_A_fields = {
        'genesymbol': 'symbol',
        'geneid': 'entrezgene',
    }
    interactor_B_fields = {
        'diseaseid': 'mesh',
        'diseasename': 'diseasename',
    }

    @staticmethod
    def parse_tsv_file(f):
        """
        Parse a tab-separated DisGeNET file opened in binary mode.
        :param f: file opened for reading in binary mode
        :return: yields a generator of parsed objects
        """

        cache = {}

        for (i, line) in enumerate(f):
            # If the line returned is byptes instead
            # of a a string then it needs to be decoded
            if isinstance(line, (bytes, bytearray)):
                line = line.decode("utf-8")
            line = line.strip('\n')

            # The following commented line contains the column headers
            if i == 0:
                line = line.replace("# ", '')  # Delete the comment prefix
                header_dict = dict(enumerate(line.split('\t')))

            # subsequent lines contain row data
            elif i >= 1:
                _r = {}
                for (pos, val) in enumerate(line.split('\t')):
                    _r[header_dict[pos]] = val
                id, r = DisGeNETParser.parse_tsv_line(i, _r)

                # Add the id and record to the cache
                if id not in cache.keys():
                    cache[id] = [r]
                else:
                    cache[id] = [r] + cache[id]

        return DisGeNETParser.collapse_cache(cache)

    @staticmethod
    def collapse_cache(cache):
        """
        The following code transforms the cache from a dictionary
        an abbreviate format with two interactors per record.
        Additional metadata is also included as a list
        :param cache:
        :return:
        """
        l = []
        for k in cache.keys():
            r = {}
            r['_id'] = k
            abbreviated_cache = []
            abbreviated_cache_repr = []

            # lists for interactors a and b that will be collapased
            int_a_list = []
            int_b_list = []

            for c in cache[k]:
                if 'interactor_a' in c.keys() and 'interactor_b' in c.keys() and 'direction' in c.keys():
                    if c['direction'] == 'A->B':
                        int_a_list.append(c['interactor_a'])
                        int_b_list.append(c['interactor_b'])
                        c.pop('interactor_a')
                        c.pop('interactor_b')
                    if c['direction'] == 'B->A':
                        int_a_list.append(c['interactor_b'])
                        int_b_list.append(c['interactor_a'])
                        c.pop('interactor_a')
                        c.pop('interactor_b')

                    c_repr = json.dumps(c, sort_keys=True)
                    if c_repr not in abbreviated_cache_repr:
                        abbreviated_cache.append(c)
                        abbreviated_cache_repr.append(c_repr)
                    else:
                        # The following block looks for duplicates in the dataset and
                        # logs an error if it finds them
                        logging.error("An evidence entry for %s is represented more than once." % k)

            r['disgenet'] = abbreviated_cache

            r['interactor_a'] = int_a_list[0]
            r['interactor_b'] = int_b_list[0]

            if not r['disgenet']:
                continue

            yield r

    @staticmethod
    def parse_tsv_line(line_num, line_dict):
        """
        Parse a dictionary representing a tsv line with a key, value pair for
        each column in the tsv file.
        :param line_dict: a tsv line dictionary
        :return: a dictionary representing a parsed DisGeNET record
        """
        # Replace all empty fields with None
        r = {k: v if v != DisGeNETParser.EMPTY_FIELD else None for k, v in line_dict.items()}

        r = DisGeNETParser.rename_fields(r, DisGeNETParser.rename_map)
        r = DisGeNETParser.parse_int_fields(r, DisGeNETParser.int_fields)
        r['score'] = DisGeNETParser.safe_float(r['score'])

        r = DisGeNETParser.group_fields(r, 'interactor_a', DisGeNETParser.interactor_A_fields)
        r = DisGeNETParser.group_fields(r, 'interactor_b', DisGeNETParser.interactor_B_fields)
        r = DisGeNETParser.sweep_record(r)

        id, r = DisGeNETParser.set_id(r)

        return id, r

    @staticmethod
    def set_id(r):
        """
        Set the id field for the record.  Interactors a and b are ordered by their
        entrez gene identifier.  A modifies record with an id is returned.
        :param r:
        :return:
        """
        if 'entrezgene' in r['interactor_a'].keys() and 'mesh' in r['interactor_b'].keys():
            id_a = int(r['interactor_a']['entrezgene'])
            id_b = r['interactor_b']['mesh']
            id = 'entrezgene:{0}-mesh:{1}'.format(id_a, id_b)
            r['direction'] = 'A->B'
        else:
            id = None
        return id, r
