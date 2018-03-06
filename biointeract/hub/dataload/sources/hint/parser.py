"""
BiogridParser parses the HiNT data file and yields
a generated dictionary of record values.

For a description of the HiNT file format see the
following link:

http://hint.yulab.org/download/HomoSapiens/binary/hq/

Source Project:   biothings.interactions
Author:  Greg Taylor:  greg.k.taylor@gmail.com
"""
import re
import operator

from hub.dataload.BiointeractParser import BiointeractParser
import biothings_client


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
        Parse a tab-separated hint file opened in binary mode.
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
                id, r = HiNTParser.parse_tsv_line(i, _r)
                result.append(r)

        result = HiNTParser.extract_interactors(result, 'hint')
        result = HiNTParser.replace_uniprot_entrez(result)
        result = HiNTParser.collapse_duplicate_keys(result, 'hint')

        # Finally, return the result
        for r in result:
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
        r = HiNTParser.group_fields(r, 'interactor_a', HiNTParser.interactor_A_fields)
        r = HiNTParser.group_fields(r, 'interactor_b', HiNTParser.interactor_B_fields)
        r = HiNTParser.sweep_record(r)

        return None, r

    @staticmethod
    def parse_evidence(r):
        evidence = []
        for e in r['pmid:method:quality'].split(HiNTParser.SEPARATOR):
            pubmed = e.split(':')
            evidence_record = {
                'pmid': int(pubmed[0]),
                'method': int(pubmed[1]),
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
        curie_a = 'entrezgene'
        id_a = r['interactor_a']['entrezgene']
        curie_b = 'entrezgene'
        id_b = r['interactor_b']['entrezgene']

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

    @staticmethod
    def build_entrezgenes(result_list):
        """
        Build a dictionary of entrezgenes for each uniprot
        :param uniprots:
        :return:
        """
        # Build the set of Uniprot entries to query
        uniprots = set()
        for r in result_list:
            uniprots.add(r['interactor_a']['uniprot'])
            uniprots.add(r['interactor_b']['uniprot'])

        # Query MyGene.info
        mg = biothings_client.get_client('gene')
        qr = mg.querymany(list(uniprots), scopes='uniprot', species='human')

        # Build the Entrezgene dictionary to return
        entrezgenes = {}
        for q in qr:
            if 'query' in q and 'entrezgene' in q:
                entrezgenes[q['query']] = q['entrezgene']
        return entrezgenes

    @staticmethod
    def replace_uniprot_entrez(result_list):
        """
        Analyze the result set for uniprot ids that will be translated to
        entrezgene ids.
        :param result_list:
        :return:
        """
        entrezgenes = HiNTParser.build_entrezgenes(result_list)
        pruned_result = []
        for r in result_list:
            # Entrezgene entries must be available for both interactor_a and interactor_b
            if r['interactor_a']['uniprot'] in entrezgenes:
                if r['interactor_b']['uniprot'] in entrezgenes:
                    r['interactor_a']['entrezgene'] = entrezgenes[r['interactor_a']['uniprot']]
                    r['interactor_b']['entrezgene'] = entrezgenes[r['interactor_b']['uniprot']]
                    id, r = HiNTParser.set_id(r)
                    pruned_result.append(r)
        return pruned_result
