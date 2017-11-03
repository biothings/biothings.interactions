"""
parse_ConsensusPathDB parses the ConsensusPathDB_human_PPI data file and yields
a generated dictionary of line values.

Source Project:   biothings.interactions
Author:  Greg Taylor:  greg.k.taylor@gmail.com
"""


def parse_ConsensusPathDB(f):
    for (i, line) in enumerate(f):
        line = line.strip('\n')

        # The first commented line is the database description

        # The second commented line contains the column headers
        if i == 1:
            line = line.replace("#  ", '')  # Delete the comment prefix
            header_dict = dict([(p, re.sub(r'\s', '_', h.lower())) for (p, h) in enumerate(line.split('\t'))])
            print(header_dict)

        # All subsequent lines contain row data
        elif i > 1:
            _r = {}
            for (pos, val) in enumerate(line.split('\t')):
                if val:
                    _r[header_dict[pos]] = val if '","' not in val else val.strip('"').split('","')
            yield _r