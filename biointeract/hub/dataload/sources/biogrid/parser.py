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
from biothings.utils.dataload import dict_sweep

class BiogridParser(BiointeractParser):
    # Static Constants
    EMPTY_FIELD = '-'
    SEPARATOR = '|'

    rename_map = {
        'Entrez Gene Interactor A': 'entrezgene_interactor_a',
        'Entrez Gene Interactor B': 'entrezgene_interactor_b',
        'Official Symbol Interactor A': 'symbol_interactor_a',
        'Official Symbol Interactor B': 'symbol_interactor_b',
        'Pubmed ID': 'pubmed',
        'Organism Interactor A': 'taxid_interactor_a',
        'Organism Interactor B': 'taxid_interactor_b',
        'Source Database': 'src_db'
    }
    int_fields = [
        'biogrid_interaction_id',
        'entrezgene_interactor_a',
        'entrezgene_interactor_b',
        'biogrid_id_interactor_a',
        'biogrid_id_interactor_b',
        'pubmed',
        'taxid_interactor_a',
        'taxid_interactor_b'
    ]
    ###############################################################
    # Fields to be grouped into single documents within each record
    ###############################################################
    interactor_A_fields = {
        'entrezgene_interactor_a': 'entrezgene',
        'biogrid_id_interactor_a': 'biogrid_id',
        'systematic_name_interactor_a': 'systematic_name',
        'symbol_interactor_a': 'symbol',
        'synonyms_interactor_a': 'synonyms',
        'taxid_interactor_a': 'taxid'
    }
    interactor_B_fields = {
        'entrezgene_interactor_b': 'entrezgene',
        'biogrid_id_interactor_b': 'biogrid_id',
        'systematic_name_interactor_b': 'systematic_name',
        'symbol_interactor_b': 'symbol',
        'synonyms_interactor_b': 'synonyms',
        'taxid_interactor_b': 'taxid'
    }
    citation_fields = {
        'author': 'author',
        'pubmed': 'pubmed'
    }
    experiment_fields = {
        'experimental_system': 'system',
        'experimental_system_type': 'system_type',
        'throughput': 'throughput'
    }

    @staticmethod
    def parse_biogrid_tsv_file(f):
        """
        Parse a tab-separated biogrid file opened in binary mode.
        :param f: file opened for reading in binary mode
        :return: yields a generator of parsed objects
        """

        result = []

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
                id, r = BiogridParser.parse_biogrid_tsv_line(i, _r)
                if r:
                    result.append(r)

        result = BiogridParser.extract_interactors(result, 'biogrid')
        result = BiogridParser.collapse_duplicate_keys(result, 'biogrid')

        # return the results
        for r in result:
            yield r

    @staticmethod
    def parse_biogrid_tsv_line(line_num, line_dict):
        """
        Parse a dictionary representing a tsv line with a key, value pair for
        each column in the tsv file.
        :param line_dict: a tsv line dictionary
        :return: a dictionary representing a parsed biogrid record
        """
        # Replace all empty fields with None
        r = {k: v if v != BiogridParser.EMPTY_FIELD else None for k, v in line_dict.items()}

        r = BiogridParser.rename_fields(r, BiogridParser.rename_map)

        r = BiogridParser.parse_int_fields(r, BiogridParser.int_fields)

        r['score'] = BiogridParser.safe_float(r['score'])

        r['synonyms_interactor_a'] = BiogridParser.parse_list(r['synonyms_interactor_a'], BiogridParser.SEPARATOR)
        r['synonyms_interactor_b'] = BiogridParser.parse_list(r['synonyms_interactor_b'], BiogridParser.SEPARATOR)
        r['phenotypes'] = BiogridParser.parse_list(r['phenotypes'], BiogridParser.SEPARATOR)
        r['qualifications'] = BiogridParser.parse_list(r['qualifications'], BiogridParser.SEPARATOR)

        r = BiogridParser.group_fields(r, 'interactor_a', BiogridParser.interactor_A_fields)
        r = BiogridParser.group_fields(r, 'interactor_b', BiogridParser.interactor_B_fields)
        r = BiogridParser.group_fields(r, 'citation', BiogridParser.citation_fields)
        r = BiogridParser.group_fields(r, 'experiment', BiogridParser.experiment_fields)

        r = BiogridParser.sweep_record(r)
        id, r = BiogridParser.set_id(r)

        return id, r

    @staticmethod
    def set_id(r):
        """
        Set the id field for the record.  Interactors a and b are ordered by their
        entrez gene identifier.  A modifies record with an id is returned.
        :param r:
        :return:
        """
        if 'entrezgene' in r['interactor_a'] and 'entrezgene' in r['interactor_b']:
            curie_a = 'entrezgene'
            id_a = int(r['interactor_a']['entrezgene'])
            curie_b = 'entrezgene'
            id_b = int(r['interactor_b']['entrezgene'])

            r['interactor_a']['entrezgene'] = id_a
            r['interactor_b']['entrezgene'] = id_b

            if id_a < id_b:
                id = '{0}:{1}-{2}:{3}'.format(curie_a, id_a, curie_b, id_b)
                r['direction'] = 'A->B'
            else:
                id = '{0}:{1}-{2}:{3}'.format(curie_b, id_b, curie_a, id_a)
                r['direction'] = 'B->A'

            # set the id and return
            r['_id'] = id
            return id, r
        else:
            return None, None
