import os, biothings, config
biothings.config_for_app(config)

from config import DATA_ARCHIVE_ROOT
from biothings.hub.dataload.dumper import LastModifiedHTTPDumper


class CTDChemGeneDumper(LastModifiedHTTPDumper):

    SRC_NAME = "CTD_chem_gene_ixns"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    CWD_DIR = '/data/CTD_chem_gene_ixns'
    SUFFIX_ATTR = "timestamp"
    SCHEDULE = "0 9 * * *"

    # Development URL - avoid hitting the CTD site repeatedly
    # SRC_URLS = ["http://biothings-data/CTD_chem_gene_ixns.tsv.gz"]

    # Production URL
    SRC_URLS = ["http://ctdbase.org/reports/CTD_chem_gene_ixns.tsv.gz"]

