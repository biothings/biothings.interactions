"""
CPDParser parses the ConsensusPathDB_human_PPI data file and yields
a generated dictionary of values.

Source Project:   biothings.interactions
Author:  Greg Taylor:  greg.k.taylor@gmail.com
"""
import re
from hub.dataload.BiointeractParser import BiointeractParser


class CPDParser(BiointeractParser):
    # Static Constants
    EMPTY_FIELD = 'NA'
    SEPARATOR = ','
    HUMAN = '_HUMAN'

    @staticmethod
    def parse_interaction_participants(entry):
        """
        Parse all interaction participants given as string from the tsv file.
        The resulting participant identifier strings will be returned with a
        trailing '_HUMAN' removed at the end.
        :param entry: a string representing the list
        :return: list of strings
        """
        vals = CPDParser.parse_list(entry, CPDParser.SEPARATOR)
        return list(map((lambda x: x.replace(CPDParser.HUMAN, '')), vals)) if vals else None

    @staticmethod
    def parse_interaction_publications(entry):
        """
        Parse all interaction publications given as a string from the tsv file.
        The resulting publication identifier  strings will be converted to a
        list of integers representing pubmed identifiers.
        :param entry: a string representing the list
        :return: list of integers
        """
        vals = CPDParser.parse_list(entry, CPDParser.SEPARATOR)
        return list(map(CPDParser.safe_int, vals)) if vals else None

    @staticmethod
    def parse_source_databases(entry):
        """
        Parse all source databases given as a string from the tsv file.
        :param entry: a string representing the list
        :return: list of strings
        """
        return CPDParser.parse_list(entry, CPDParser.SEPARATOR)

    @staticmethod
    def parse_cpd_tsv_line(line_dict):
        """
        Parse a dictionary representing a tsv line with a key, value pair for
        each column in the tsv file.
        :param line_dict: a tsv line dictionary
        :return: a dictionary representing a parsed biogrid record
        """
        # Replace all empty fields with None
        r = {k: v if v != CPDParser.EMPTY_FIELD else None for k, v in line_dict.items()}

        r['interaction_confidence'] = CPDParser.safe_float(r['interaction_confidence'])
        r['interaction_participants'] = CPDParser.parse_interaction_participants(r['interaction_participants'])
        r['interaction_publications'] = CPDParser.parse_interaction_publications(r['interaction_publications'])
        r['source_databases'] = CPDParser.parse_source_databases(r['source_databases'])
        return r

    @staticmethod
    def parse_cpd_tsv_file(f):
        """
        Parse a tab-separated biogrid file opened in binary mode.
        :param f: file opened for reading in binary mode
        :return: yields a generator of parsed objects
        """
        for (i, line) in enumerate(f):
            line = line.strip('\n')

            # The first commented line is the database description

            # The second commented line contains the column headers
            if i == 1:
                line = line.replace("#  ", '')  # Delete the comment prefix
                header_dict = dict(enumerate(line.split('\t')))
                print(header_dict)

            # All subsequent lines contain row data
            elif i > 1:
                _r = {}
                for (pos, val) in enumerate(line.split('\t')):
                    _r[header_dict[pos]] = val
                yield CPDParser.parse_cpd_tsv_line(_r)
