import os, biothings, config_hub
biothings.config_for_app(config_hub)

from config_hub import DATA_ARCHIVE_ROOT
from biothings.hub.dataload.dumper import LastModifiedHTTPDumper


class HiNTDumper(LastModifiedHTTPDumper):

    SRC_NAME = "hint"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    CWD_DIR = '/data/hint'
    SUFFIX_ATTR = "timestamp"
    SCHEDULE = "0 9 * * *"

    # Development URL - avoid hitting the HiNT site repeatedly
    # SRC_URLS = ["http://biothings-data/HomoSapiens_htb_hq.txt"]

    # Production URL
    SRC_URLS = ["http://hint.yulab.org/download/HomoSapiens/binary/hq/"]

