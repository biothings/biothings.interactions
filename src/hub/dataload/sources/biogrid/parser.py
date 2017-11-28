"""
parse_ConsensusPathDB parses the ConsensusPathDB_human_PPI data file and yields
a generated dictionary of line values.

Source Project:   biothings.interactions
Author:  Greg Taylor:  greg.k.taylor@gmail.com
"""
import re


def parse_biogrid(f):
    empty_field = '-'
    separator = '|'

    for (i, line) in enumerate(f):
        line = line.decode("utf-8")
        line = line.strip('\n')

      # The first commented line contains the column headers
        if i == 0:
            line = line.replace("#", '')  # Delete the comment prefix
            header_dict = dict([(p, re.sub(r'\s', '_', h.lower())) for (p, h) in enumerate(line.split('\t'))])
            print(header_dict)

        # All subsequent lines contain row data
        elif i > 0:
            _r = {}
            for (pos, val) in enumerate(line.split('\t')):
                if val and val != empty_field:
                    _r[header_dict[pos]] = val if separator not in val else val.split(separator)
            yield _r