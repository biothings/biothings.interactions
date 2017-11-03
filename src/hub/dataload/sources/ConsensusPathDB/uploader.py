import os
import biothings.hub.dataload.uploader as uploader

from .parser import parse_ConsensusPathDB


class ConsensusPathDBUploader(uploader.BaseSourceUploader):

    main_source = "ConsensusPathDB"
    name = "human_PPI"

    def load_data(self, data_folder):
        consensus_file = os.path.join(data_folder,"ConsensusPathDB_human_PPI")
        self.logger.info("Load data from file '%s'" % consensus_file)
        return parse_ConsensusPathDB(open(consensus_file))