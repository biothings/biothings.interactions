"""
The following script builds the ElasticSearch Index for the protein-protein interaction (PPI) data source.  This
script reads through the data source file taking the first line which it assumes to be column headers.  It then
reads through the rest of the file using biothings ElasticSearch indexer
Source Project:   http://consensuspathdb.org/
Author:  Greg Taylor:  greg.k.taylor@gmail.com
"""
import os
import logging
import re
import sys
from biothings.web.index_base import options
from www.settings import MyHuman_PpiWebSettings
from biothings.utils.es import ESIndexer


# *****************************************************************************
# Settings
# *****************************************************************************
web_settings = MyHuman_PpiWebSettings(config='config')
# Instantiate settings class to read the ES index and document names


# *****************************************************************************
# Logging
# *****************************************************************************
# Setup the logger for the Script
logging.basicConfig()
logger = logging.getLogger()


# *****************************************************************************
# load_data
# *****************************************************************************
# load_data parses the ConsensusPathDB_human_PPI data file and yields
# a generated dictionary of line values.
def load_data(f):
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


# *****************************************************************************
# Command line parsing
# *****************************************************************************
# Obtain the filename from from the command line argument
if len(sys.argv) != 2:
    logger.info("Usage:  build_index.py <path_to_file>/ConsensusPathDB_human_PPI")
    exit()
human_ppi_datafile = sys.argv[1]
logger.info("Attempting to index the provided file:{}".format(human_ppi_datafile))

# *****************************************************************************
# Data file existence
# *****************************************************************************
if not os.path.isfile(human_ppi_datafile):
    logger.error("Unabled to read the provided file {} for reading.".format(human_ppi_datafile))
    exit()

# *****************************************************************************
# Index the target data file
# *****************************************************************************
try:
    indexer = ESIndexer(index=web_settings.ES_INDEX, doc_type=web_settings.ES_DOC_TYPE, es_host=web_settings.ES_HOST)
    indexer.create_index(mapping={web_settings.ES_DOC_TYPE: {'dynamic': True}})
    with open(human_ppi_datafile, 'r') as gene_file:
        indexer.index_bulk(load_data(gene_file))
        logger.info("The ElasticSearch Indexer loaded {} entries.".format(indexer.count()))

except UnboundLocalError:
        logger.error("An error occurred likely related to the column headers in the file.".format(human_ppi_datafile))
