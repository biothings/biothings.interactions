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
from hub.dataload.BiointeractParser import BiointeractParser


class BiogridParser(BiointeractParser):
    # Static Constants
    EMPTY_FIELD = '-'
    SEPARATOR = '|'

    ###############################################################
    # Fields to be grouped into single documents within each record
    ###############################################################
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
    interactor_A_fields = {
        'Entrez Gene Interactor A': 'Entrez Gene',
        'BioGRID ID Interactor A': 'BioGRID ID',
        'Systematic Name Interactor A': 'Systematic Name',
        'Official Symbol Interactor A': 'Official Symbol',
        'Synonyms Interactor A': 'Synonyms',
        'Organism Interactor A': 'Organism'
    }
    interactor_B_fields = {
        'Entrez Gene Interactor B': 'Entrez Gene',
        'BioGRID ID Interactor B': 'BioGRID ID',
        'Systematic Name Interactor B': 'Systematic Name',
        'Official Symbol Interactor B': 'Official Symbol',
        'Synonyms Interactor B': 'Synonyms',
        'Organism Interactor B': 'Organism'
    }
    citation_fields = {
        'Author': 'Author',
        'Pubmed ID': 'Pubmed ID'
    }
    experiment_fields = {
        'Experimental System': 'System',
        'Experimental System Type': 'System Type'
    }


    @staticmethod
    def parse_biogrid_tsv_file(f):
        """
        Parse a tab-separated biogrid file opened in binary mode.
        :param f: file opened for reading in binary mode
        :return: yields a generator of parsed objects
        """
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

        r = BiogridParser.parse_int_fields(r, BiogridParser.int_fields)

        r['Score'] = BiogridParser.safe_float(r['Score'])

        r['Synonyms Interactor A'] = BiogridParser.parse_list(r['Synonyms Interactor A'], BiogridParser.SEPARATOR)
        r['Synonyms Interactor B'] = BiogridParser.parse_list(r['Synonyms Interactor B'], BiogridParser.SEPARATOR)
        r['Phenotypes'] = BiogridParser.parse_list(r['Phenotypes'], BiogridParser.SEPARATOR)
        r['Qualifications'] = BiogridParser.parse_list(r['Qualifications'], BiogridParser.SEPARATOR)

        r = BiogridParser.group_fields(r, 'Interactor A', BiogridParser.interactor_A_fields)
        r = BiogridParser.group_fields(r, 'Interactor B', BiogridParser.interactor_B_fields)
        r = BiogridParser.group_fields(r, 'Citation', BiogridParser.citation_fields)
        r = BiogridParser.group_fields(r, 'Experiment', BiogridParser.experiment_fields)

        return r