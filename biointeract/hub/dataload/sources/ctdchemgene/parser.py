"""
CTD parses the CTD data file and yields
a generated dictionary of record values.

For a description of the CTD file format see the
following link:

http://ctdbase.org/downloads/;jsessionid=0BD8D8C07B7661002359C02D7C0275F8

Source Project:   biothings.interactions
Author:  Greg Taylor:  greg.k.taylor@gmail.com
"""
import re
import operator

from hub.dataload.BiointeractParser import BiointeractParser
from biothings.utils.dataload import dict_sweep

class CTDChemGeneParser(BiointeractParser):
    # Static Constants
    EMPTY_FIELD = '-'
    SEPARATOR = '|'

    # all default field names are ok
    rename_map = {
    }

    int_fields = [
        'pubmedids'
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
        'chemicalid': 'chemicalid',
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

        #########################################
        # Transform cache from dictionary to list
        #########################################
        # The following code trnsforms the cache from a dictionary
        # an abbreviate format with two interactors per record.
        # Additional metadata is also included as a list
        l = []
        for k in cache.keys():
            r = {}
            r['_id'] = k
            abbreviated_cache = []
            for c in cache[k]:
                if 'interactor_a' in c.keys() and 'interactor_b' in c.keys() and 'direction' in c.keys():
                    if c['direction'] == 'A->B':
                        r['interactor_a'] = c['interactor_a']
                        r['interactor_b'] = c['interactor_b']
                        c.pop('interactor_a')
                        c.pop('interactor_b')
                    if c['direction'] == 'B->A':
                        r['interactor_a'] = c['interactor_b']
                        r['interactor_b'] = c['interactor_a']
                        c.pop('interactor_a')
                        c.pop('interactor_b')
                    abbreviated_cache.append(c)

            r['ctd'] = abbreviated_cache
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

        r['pubmedids'] = CTDChemGeneParser.parse_list(r['pubmedids'], CTDChemGeneParser.SEPARATOR)
        r['pubmedids'] = [CTDChemGeneParser.safe_int(x) for x in r['pubmedids']]

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
        if 'entrezgene' in r['interactor_a'].keys() and 'chemicalid' in r['interactor_b'].keys():
            id_a = int(r['interactor_a']['entrezgene'])
            id_b = r['interactor_b']['chemicalid']
            id = 'entrezgene:{0}-mesh:{1}'.format(id_a, id_b)
            r['direction'] = 'A->B'
        else:
            id = None
        return id, r

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
    def sweep_record(r):
        """
        Remove all None fields from a record
        :param r:
        :return:
        """
        r2 = {}
        for k1 in r.keys():
            if isinstance(r[k1], dict):
                r2[k1] = {}
                for k2 in r[k1]:
                    if r[k1][k2]:
                        r2[k1][k2] = r[k1][k2]
            elif r[k1]:
                r2[k1] = r[k1]
        return r2
