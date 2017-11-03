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
from hub.dataload.sources.ConsensusPathDB.parser import parse_ConsensusPathDB


# *****************************************************************************
# Settings
# *****************************************************************************
web_settings = MyHuman_PpiWebSettings(config='config_www')
# Instantiate settings class to read the ES index and document names


# *****************************************************************************
# Logging
# *****************************************************************************
# Setup the logger for the Script
logging.basicConfig()
logger = logging.getLogger()


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
# Biothings uploader - upload data from the file into MongoDB
# *****************************************************************************
consensus_uploader = uploader.ConsensusPathDBUploader()
consensus_uploader.load_data("../data/")

# *****************************************************************************
# Index the target data file
# *****************************************************************************
# try:
#     indexer = ESIndexer(index=web_settings.ES_INDEX, doc_type=web_settings.ES_DOC_TYPE, es_host=web_settings.ES_HOST)
#     indexer.create_index(mapping={web_settings.ES_DOC_TYPE: {'dynamic': True}})
#     with open(human_ppi_datafile, 'r') as gene_file:
#         indexer.index_bulk(parse_ConsensusPathDB(gene_file))
#         logger.info("The ElasticSearch Indexer loaded {} entries.".format(indexer.count()))
#
# except UnboundLocalError:
#         logger.error("An error occurred likely related to the column headers in the file.".format(human_ppi_datafile))
