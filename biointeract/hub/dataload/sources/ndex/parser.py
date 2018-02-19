"""
nDEXParser parses the nDEX data file and yields
a generated dictionary of record values.

Source Project:   biothings.interactions
Author:  Greg Taylor:  greg.k.taylor@gmail.com
"""
import json
import re
import operator

from hub.dataload.BiointeractParser import BiointeractParser


class nDEXParser(BiointeractParser):

    # nDEX Index Constants
    NODES = 4
    EDGES = 5
    NODEATTRIBUTES = 7
    EDGEATTRIBUTES = 8

    @staticmethod
    def parse_ndex_file(f):
        """
        Parse an nDEX CX file opened in binary mode.
        :param f: file opened for reading in binary mode
        :return: yields a generator of parsed objects
        """

        cache = {}

        j = json.loads(f.read())

        # read nodes and nodeAttributes
        n = nDEXParser.read_nodes(j)
        na = nDEXParser.read_node_attributes(j)

        # assemble the information
        edge_types = set()
        complex_count = 0
        interactions = []
        for i in range(len(j[nDEXParser.EDGES]['edges'])):
            edge_types.add(j[nDEXParser.EDGES]['edges'][i]['i'])
            if j[nDEXParser.EDGES]['edges'][i]['i'] == 'in-complex-with':
                complex_count = complex_count + 1
                interaction = {
                    'interactor_a': {
                        'ndex': n[j[nDEXParser.EDGES]['edges'][i]['s']]['@id']
                    },
                    'interactor_b': {
                        'ndex': n[j[nDEXParser.EDGES]['edges'][i]['t']]['@id']
                    },
                    'ndex': {
                        'cx_edge_id': j[nDEXParser.EDGES]['edges'][i]['@id'],
                        'edge': j[nDEXParser.EDGES]['edges'][i],
                        's': n[j[nDEXParser.EDGES]['edges'][i]['s']],
                        'sa': na[j[nDEXParser.EDGES]['edges'][i]['s']],
                        't': n[j[nDEXParser.EDGES]['edges'][i]['t']],
                        'ta': na[j[nDEXParser.EDGES]['edges'][i]['t']],
                    }
                }

                id, interaction = nDEXParser.set_id(interaction)
                interaction['_id'] = id

                yield interaction

    @staticmethod
    def set_id(r):
        """
        Set the id field for the record.  Interactors a and b are ordered by their
        entrez gene identifier.  A modifies record with an id is returned.
        :param r:
        :return:
        """
        curie_a, entrez_a = ('ndex', r['interactor_a']['ndex'])
        curie_b, entrez_b = ('ndex', r['interactor_b']['ndex'])

        if str(entrez_a) < str(entrez_b):
            id = '{0}:{1}-{2}:{3}'.format(curie_a, entrez_a, curie_b, entrez_b)
            r['direction'] = 'A->B'
        else:
            id = '{0}:{1}-{2}:{3}'.format(curie_b, entrez_b, curie_a, entrez_a)
            r['direction'] = 'B->A'

        return id, r

    def read_nodes(j):
        """
        read Nodes from the nDEX CS file
        :return:
        """
        n = {}
        for i in range(len(j[nDEXParser.NODES]['nodes'])):
            n[j[nDEXParser.NODES]['nodes'][i]['@id']] = j[nDEXParser.NODES]['nodes'][i]
        return n

    def read_node_attributes(j):
        """
        read Node Attributes from the nDEX CS file
        :return:
        """
        a = {}
        for i in range(len(j[nDEXParser.NODEATTRIBUTES]['nodeAttributes'])):
            attr = j[nDEXParser.NODEATTRIBUTES]['nodeAttributes'][i]
            key = j[nDEXParser.NODEATTRIBUTES]['nodeAttributes'][i]['po']

            if attr and key:
                if key in a.keys():
                    a[key].append(attr)
                else:
                    a[key] = [attr]
        return a
