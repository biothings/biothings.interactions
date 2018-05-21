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

    # Production URL
    SRC_URLS = ["http://public.ndexbio.org/v2/network/09f3c90a-121a-11e6-a039-06603eb7f303"]
