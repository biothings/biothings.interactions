"""
BiogridParser parses the biogrid data file and yields
a generated dictionary of record values.

Source Project:   biothings.interactions
Author:  Greg Taylor:  greg.k.taylor@gmail.com
"""
import re


class BiogridParser(object):
    # Static Constants
    EMPTY_FIELD = '-'
    SEPARATOR = '|'

    @staticmethod
    def parse_biogrid_tsv_file(f):
        """
        Parse a tab-separated biogrid file opened in binary mode.
        :param f: file opened for reading in binary mode
        :return: yields a generator of parsed objects
        """
        for (i, line) in enumerate(f):
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
                yield BiogridParser.parse_biogrid_tsv_line(_r)

    @staticmethod
    def parse_biogrid_tsv_line(line_dict):
        """
        Parse a dictionary representing a tsv line with a key, value pair for
        each column in the tsv file.
        :param line_dict: a tsv line dictionary
        :return: a dictionary representing a parsed biogrid record
        """
        # Replace all empty fields with None
        r = {k: v if v != BiogridParser.EMPTY_FIELD else None for k, v in line_dict.items()}

        int_fields = [
            'BioGRID Interaction ID',
            'Entrez Gene Interactor A',
            'Entrez Gene Interactor B',
            'BioGRID ID Interactor A',
            'BioGRID ID Interactor B',
            'Pubmed ID',
            'Organism Interactor A',
            'Organism Interactor B'
        ]

        r = BiogridParser.parse_int_fields(r, int_fields)
        r['Synonyms Interactor A'] = BiogridParser.parse_synonyms(r['Synonyms Interactor A'])
        r['Synonyms Interactor B'] = BiogridParser.parse_synonyms(r['Synonyms Interactor B'])
        return r

    @staticmethod
    def parse_synonyms(entry):
        """
        Parse all synonyms given as string from the tsv file.
        The resulting participant identifier strings will be returned,
        :param entry: a string representing the list
        :return: list of strings
        """
        return [x for x in entry.split(BiogridParser.SEPARATOR)] if entry else None

    @staticmethod
    def parse_int_fields(record, int_fields):
        """
        Parse integer fields in a biogrid record dictionary
        :param entry: a record dictionary
        :return: a converted record dictionary
        """
        for field in int_fields:
            record[field] = BiogridParser.safe_int(record[field])
        return record

    @staticmethod
    def safe_int(str):
        """
        Utility function to convert a string to an integer returning 0 if the
        conversion of unsucessful.
        :param str:
        :return:
        """
        try:
            return int(str)
        except ValueError:
            return 0
