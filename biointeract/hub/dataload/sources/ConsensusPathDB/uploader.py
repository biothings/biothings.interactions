import os
import gzip

import biothings, config
biothings.config_for_app(config)

import biothings.hub.dataload.uploader as uploader
from .parser import CPDParser


class ConsensusPathDBUploader(uploader.BaseSourceUploader):

    # main_source = "ConsensusPathDB"
    name = "ConsensusPathDB"
    collection_name = "ConsensusPathDB_human_PPI"
    zip_file_name = "ConsensusPathDB_human_PPI.gz"
    __metadata__ = {"mapper": 'consensuspathdb_mapper'}

    def load_data(self, data_folder):
        consensus_file = os.path.join(data_folder, self.zip_file_name)
        self.logger.info("Load data from file '%s'" % consensus_file)
        return CPDParser.parse_cpd_tsv_file(gzip.open(consensus_file, mode='rt'))
