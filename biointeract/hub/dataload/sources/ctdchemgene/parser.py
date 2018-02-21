"""
CTD parses the CTD data file and yields
a generated dictionary of record values.

For a description of the CTD file format see the
following link:

http://ctdbase.org/downloads/;jsessionid=0BD8D8C07B7661002359C02D7C0275F8

Source Project:   biothings.interactions
Author:  Greg Taylor:  greg.k.taylor@gmail.com
"""
import json
import logging
import operator
import re

from hub.dataload.BiointeractParser import BiointeractParser
from biothings.utils.dataload import dict_sweep

class CTDChemGeneParser(BiointeractParser):
    # Static Constants
    EMPTY_FIELD = '-'
    SEPARATOR = '|'

    # all default field names are ok
    rename_map = {
        'Organism': 'tax',
        'OrganismID': 'taxid',
        'PubMedIDs': 'pubmed',
        'ChemicalID': 'mesh'
    }

    int_fields = [
        'pubmed'
    ]
    ###############################################################
    # Fields to be grouped into single documents within each record
    ###############################################################
    interactor_A_fields = {
        'genesymbol': 'symbol',
        'geneid': 'entrezgene',
        'geneforms': 'geneforms'
    }
    interactor_B_fields = {
        'chemicalname': 'chemicalname',
        'mesh': 'mesh',
        'casrn': 'casrn'
    }

    @staticmethod
    def parse_tsv_file(f):
        """
        Parse a tab-separated CTD file opened in binary mode.
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
            if i == 27:
                line = line.replace("# ", '')  # Delete the comment prefix
                header_dict = dict(enumerate(line.split('\t')))

            # subsequent lines contain row data
            elif i >= 29:
                _r = {}
                for (pos, val) in enumerate(line.split('\t')):
                    _r[header_dict[pos]] = val
                id, r = CTDChemGeneParser.parse_tsv_line(i, _r)

                # Add the id and record to the cache
                if id not in cache.keys():
                    cache[id] = [r]
                else:
                    cache[id] = [r] + cache[id]

        return CTDChemGeneParser.collapse_cache(cache)

    @staticmethod
    def collapse_cache(cache):
        """
        The following code trnsforms the cache from a dictionary
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

            r['ctd'] = abbreviated_cache

            # collapse int_a_list and int_b_list dictionaries
            int_a = int_a_list[0]
            for a in int_a_list:
                if a != int_a:
                    int_a = CTDChemGeneParser.combine_interactions(int_a, a)
            int_b = int_b_list[0]
            for b in int_b_list:
                if b != int_b:
                    int_b = CTDChemGeneParser.combine_interactions(int_b, b)
            r['interactor_a'] = int_a
            r['interactor_b'] = int_b

            yield r

    @staticmethod
    def parse_tsv_line(line_num, line_dict):
        """
        Parse a dictionary representing a tsv line with a key, value pair for
        each column in the tsv file.
        :param line_dict: a tsv line dictionary
        :return: a dictionary representing a parsed ctd record
        """
        # Replace all empty fields with None
        r = {k: v if v != CTDChemGeneParser.EMPTY_FIELD else None for k, v in line_dict.items()}

        r = CTDChemGeneParser.rename_fields(r, CTDChemGeneParser.rename_map)

        r['geneforms'] = CTDChemGeneParser.parse_list(r['geneforms'], CTDChemGeneParser.SEPARATOR)
        r['interactionactions'] = CTDChemGeneParser.parse_list(r['interactionactions'], CTDChemGeneParser.SEPARATOR)

        # parse the pubmed fields
        r['pubmed'] = CTDChemGeneParser.parse_list(r['pubmed'], CTDChemGeneParser.SEPARATOR)
        if isinstance(r['pubmed'], list):
            r['pubmed'] = [CTDChemGeneParser.safe_int(x) for x in r['pubmed']]
        else:
            r['pubmed'] = CTDChemGeneParser.safe_int(r['pubmed'])

        r = CTDChemGeneParser.group_fields(r, 'interactor_a', CTDChemGeneParser.interactor_A_fields)
        r = CTDChemGeneParser.group_fields(r, 'interactor_b', CTDChemGeneParser.interactor_B_fields)
        r = CTDChemGeneParser.sweep_record(r)

        id, r = CTDChemGeneParser.set_id(r)

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

    @staticmethod
    def combine_interactions(int1, int2):
        """
        Combine the elements in interation 2 into interaction 1.
        The method comines all elements from the second dictionary into
        the first.  The first dictionary contains either single elements
        for unique properties or lists if multiple properties or lists of
        properties need to be combined.
        :param int1:
        :param int2:
        :return:
        """
        for k in int2.keys():
            if k in int1.keys():
                if int1[k] != int2[k]:
                    if isinstance(int1[k], list):
                        if isinstance(int2[k], list):
                            for s in int2[k]:
                                if s not in int1[k]:
                                    int1[k].append(s)
                        else:
                            if int2[k] not in int1[k]:
                                int1[k].append(int2[k])
                    else:
                        int1[k] = [int1[k], int2[k]]
            else:
                int1[k] = int2[k]
        return int1
