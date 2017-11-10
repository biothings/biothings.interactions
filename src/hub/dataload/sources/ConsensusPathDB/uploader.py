import os
import gzip

import biothings, config_hub
biothings.config_for_app(config_hub)

import biothings.hub.dataload.uploader as uploader
from .parser import parse_ConsensusPathDB


class ConsensusPathDBUploader(uploader.BaseSourceUploader):

    # main_source = "ConsensusPathDB"
    name = "ConsensusPathDB"
    collection_name = "ConsensusPathDB_human_PPI"
    zip_file_name = "ConsensusPathDB_human_PPI.gz"

    def load_data(self, data_folder):
        consensus_file = os.path.join(data_folder, self.zip_file_name)
        self.logger.info("Load data from file '%s'" % consensus_file)
        return parse_ConsensusPathDB(gzip.open(consensus_file))
