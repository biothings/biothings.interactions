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
import operator

from hub.dataload.BiointeractParser import BiointeractParser
import requests


class HiNTParser(BiointeractParser):
    # Static Constants
    EMPTY_FIELD = '-'
    SEPARATOR = '|'

    rename_map = {
    }
    int_fields = [
    ]

    ###############################################################
    # Fields to be grouped into single documents within each record
    ###############################################################
    interactor_A_fields = {
        'entrezgene_a': 'entrezgene',
        'uniprot_a': 'uniprot',
        'gene_a': 'gene',
        'orf_a': 'orf',
        'alias_a': 'alias'
    }
    interactor_B_fields = {
        'entrezgene_b': 'entrezgene',
        'uniprot_b': 'uniprot',
        'gene_b': 'gene',
        'orf_b': 'orf',
        'alias_b': 'alias'
    }

    @staticmethod
    def parse_tsv_file(f):
        """
        Parse a tab-separated biogrid file opened in binary mode.
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

            # The first commented line contains the column headers
            if i == 0:
                line = line.replace("#", '')  # Delete the comment prefix
                header_dict = dict(enumerate(line.split('\t')))

            # All subsequent lines contain row data
            elif i > 0:
                _r = {}
                for (pos, val) in enumerate(line.split('\t')):
                    _r[header_dict[pos]] = val
                id, r = HiNTParser.parse_tsv_line(i, _r)

                # Add the id and record to the cache
                if id not in cache.keys():
                    cache[id] = [r]
                else:
                    cache[id] = [r] + cache[id]

        #########################################
        # Transform cache from dictionary to list
        #########################################
        # The following code transforms the cache from a dictionary
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

            r['hint'] = abbreviated_cache
            yield r

    @staticmethod
    def parse_tsv_line(line_num, line_dict):
        """
        Parse a dictionary representing a tsv line with a key, value pair for
        each column in the tsv file.
        :param line_dict: a tsv line dictionary
        :return: a dictionary representing a parsed biogrid record
        """
        # Replace all empty fields with None
        r = {k: v if v != HiNTParser.EMPTY_FIELD else None for k, v in line_dict.items()}

        r = HiNTParser.rename_fields(r, HiNTParser.rename_map)
        r = HiNTParser.parse_evidence(r)
        id, r = HiNTParser.set_id(r)
        r = HiNTParser.group_fields(r, 'interactor_a', HiNTParser.interactor_A_fields)
        r = HiNTParser.group_fields(r, 'interactor_b', HiNTParser.interactor_B_fields)
        r = HiNTParser.sweep_record(r)

        return id, r

    @staticmethod
    def parse_evidence(r):
        evidence = []
        for e in r['pmid:method:quality'].split(HiNTParser.SEPARATOR):
            pubmed = e.split(':')
            evidence_record = {
                'pmid': pubmed[0],
                'method': pubmed[1],
                'quality': pubmed[2]
            }
            evidence.append(evidence_record)
        r['evidence'] = evidence
        r.pop('pmid:method:quality')
        return r

    @staticmethod
    def set_id(r):
        """
        Set the id field for the record.  Interactors a and b are ordered by their
        entrez gene identifier.  A modifies record with an id is returned.
        :param r:
        :return:
        """
        if 'uniprot_a' in r.keys() and 'uniprot_b' in r.keys():

            curie_a, entrez_a = HiNTParser.uniprot_to_entrez(r['uniprot_a'])
            curie_b, entrez_b = HiNTParser.uniprot_to_entrez(r['uniprot_b'])

            if curie_a == 'entrezgene':
                r['entrezgene_a'] = entrez_a
            if curie_b == 'entrezgene':
                r['entrezgene_b'] = entrez_b

            if str(entrez_a) < str(entrez_b):
                id = '{0}:{1}-{2}:{3}'.format(curie_a, entrez_a, curie_b, entrez_b)
                r['direction'] = 'A->B'
            else:
                id = '{0}:{1}-{2}:{3}'.format(curie_b, entrez_b, curie_a, entrez_a)
                r['direction'] = 'B->A'
        else:
            id = None

        return id, r

    @staticmethod
    def uniprot_to_entrez(uniprot):
        """
        Convert a uniprot identifer to an entrezgene identifier if possible.  The
        curie prefix and the identifier is returned.  When converting the identifier
        the mygene.info server is used.  If the conversion fails then the uniprot
        curie and identifier is returned.
        :param uniprot: uniprot identifier
        :return:
        """
        # payload = {'q': uniprot}
        # id_r = requests.get('http://mygene.info/v3/query', params=payload)
        # if id_r.json()['hits']:
        #     curie = 'entrezgene'
        #     id = id_r.json()['hits'][0]['entrezgene']
        # else:
        #     curie = 'uniprot'
        #     id = uniprot

        curie = 'uniprot'
        id = uniprot

        return curie, id

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
        Remove all None fields from a biogrid record
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
