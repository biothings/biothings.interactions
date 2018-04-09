import os, biothings, config
biothings.config_for_app(config)

from config import DATA_ARCHIVE_ROOT
from biothings.hub.dataload.dumper import LastModifiedHTTPDumper


class nDEXDumper(LastModifiedHTTPDumper):

    SRC_NAME = "ndex"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    CWD_DIR = '/data/ndex'
    SUFFIX_ATTR = "timestamp"
    SCHEDULE = "0 9 * * *"

    # Development URL - avoid hitting the CTD site repeatedly
    SRC_URLS = ["http://biothings-data/NCI Pathway Interaction Database - Final Revision.cx"]

