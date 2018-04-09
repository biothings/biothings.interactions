import os, biothings, config
biothings.config_for_app(config)

from config import DATA_ARCHIVE_ROOT
from biothings.hub.dataload.dumper import LastModifiedHTTPDumper


class ConsensusPathDBDumper(LastModifiedHTTPDumper):

    SRC_NAME = "ConsensusPathDB"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    CWD_DIR = '/data/ConsensusPathDB'
    SUFFIX_ATTR = "timestamp"
    SCHEDULE = "0 9 * * *"

    # Development URL - avoid hitting the ConsensusPathDB site repeatedly
    # SRC_URLS = ["http://biothings-data/ConsensusPathDB_human_PPI.gz"]

    # Production URL
    SRC_URLS = ["http://cpdb.molgen.mpg.de/download/ConsensusPathDB_human_PPI.gz"]

