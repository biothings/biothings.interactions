import os
import gzip

import biothings, config
biothings.config_for_app(config)

import biothings.hub.dataload.uploader as uploader
from .parser import DisGeNETParser


class DisGeNETUploader(uploader.BaseSourceUploader):

    # main_source = "ConsensusPathDB"
    name = "disgenet"
    collection_name = "disgenet"
    zip_file_name = "curated_gene_disease_associations.tsv.gz"
    # __metadata__ = {"mapper": 'consensuspathdb_mapper'}

    def load_data(self, data_folder):
        downloaded_file = os.path.join(data_folder, self.zip_file_name)
        self.logger.info("Load data from file '%s'" % downloaded_file)
        return DisGeNETParser.parse_tsv_file(gzip.open(downloaded_file, mode='rt'))
