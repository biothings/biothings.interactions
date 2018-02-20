import os
import gzip

import biothings, config_hub
biothings.config_for_app(config_hub)

import biothings.hub.dataload.uploader as uploader
from .parser import nDEXParser


class nDEXUploader(uploader.BaseSourceUploader):

    # main_source = "ConsensusPathDB"
    name = "ndex"
    collection_name = "ndex"
    file_name = "NCI Pathway Interaction Database - Final Revision.cx"
    # __metadata__ = {"mapper": 'consensuspathdb_mapper'}

    def load_data(self, data_folder):
        downloaded_file = os.path.join(data_folder, self.file_name)
        self.logger.info("Load data from file '%s'" % downloaded_file)
        return nDEXParser.parse_ndex_file(open(downloaded_file, mode='rt'))
