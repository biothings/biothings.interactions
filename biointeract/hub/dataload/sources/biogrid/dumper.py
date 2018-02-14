import os, biothings, config_hub
biothings.config_for_app(config_hub)

from config_hub import DATA_ARCHIVE_ROOT
from biothings.hub.dataload.dumper import LastModifiedHTTPDumper


class BiogridDumper(LastModifiedHTTPDumper):

    SRC_NAME = "biogrid"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    CWD_DIR = '/data/biogrid'
    SUFFIX_ATTR = "timestamp"
    SCHEDULE = "0 9 * * *"

    # Development URL - avoid hitting the ConsensusPathDB site repeatedly
    SRC_URLS = ["http://biothings-data/BIOGRID-ALL-3.4.156.tab2.zip"]

    # Production URL
    # SRC_URLS = ["http://downloads.thebiogrid.org/Download/BioGRID/Release-Archive/BIOGRID-3.4.154/BIOGRID-ALL-3.4.154.tab2.zip"]
