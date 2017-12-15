import os, biothings, config_hub
biothings.config_for_app(config_hub)

from config_hub import DATA_ARCHIVE_ROOT
from biothings.hub.dataload.dumper import LastModifiedHTTPDumper


class ConsensusPathDBDumper(LastModifiedHTTPDumper):

    SRC_NAME = "ConsensusPathDB"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    CWD_DIR = '/data/ConsensusPathDB'
    SUFFIX_ATTR = "timestamp"
    SCHEDULE = "0 9 * * *"

    # Development URL - avoid hitting the ConsensusPathDB site repeatedly
    SRC_URLS = ["http://biothings-data/ConsensusPathDB_human_PPI.gz"]

    # Production URL
    # SRC_URLS = ["http://cpdb.molgen.mpg.de/download/ConsensusPathDB_human_PPI.gz"]
