import os, biothings, config
biothings.config_for_app(config)

from config import DATA_ARCHIVE_ROOT
from biothings.hub.dataload.dumper import LastModifiedHTTPDumper


class DisGeNETDumper(LastModifiedHTTPDumper):

    SRC_NAME = "disgenet"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    CWD_DIR = '/data/disgenet'
    SUFFIX_ATTR = "timestamp"
    SCHEDULE = "0 9 * * *"

    # Development URL - avoid hitting the CTD site repeatedly
    # SRC_URLS = ["http://biothings-data/curated_gene_disease_associations.tsv.gz"]

    # Production URL
    SRC_URLS = ["http://www.disgenet.org/ds/DisGeNET/results/curated_gene_disease_associations.tsv.gz"]

