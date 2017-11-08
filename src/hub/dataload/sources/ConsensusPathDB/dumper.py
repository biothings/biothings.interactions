import os, biothings, config_hub
biothings.config_for_app(config_hub)

from config_hub import DATA_ARCHIVE_ROOT
from biothings.hub.dataload.dumper import LastModifiedHTTPDumper


class ConsensusPathDBDumper(LastModifiedHTTPDumper):

    SRC_NAME = "ConsensusPathDB"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    CWD_DIR = '/c/dev/biothings.interactions/data/ConsensusPathDB'
    SUFFIX_ATTR = "timestamp"
    SCHEDULE = "0 9 * * *"
    SRC_URLS = ["http://cpdb.molgen.mpg.de/download/ConsensusPathDB_human_PPI.gz"]

    def create_todump_list(self, force=False):
        file_to_dump = "ConsensusPathDB_human_PPI.gz"
        new_localfile = os.path.join(self.new_data_folder, file_to_dump)
        try:
            current_localfile = os.path.join(self.current_data_folder, file_to_dump)
        except TypeError:
            # current data folder doesn't even exist
            current_localfile = new_localfile
        if force or not os.path.exists(current_localfile) or self.remote_is_better(file_to_dump, current_localfile):
            # register new release (will be stored in backend)
            self.to_dump.append({"remote": file_to_dump, "local": new_localfile})
