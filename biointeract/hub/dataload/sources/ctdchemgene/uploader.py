import os
import gzip

import biothings, config
biothings.config_for_app(config)

import biothings.hub.dataload.uploader as uploader
from .parser import CTDChemGeneParser


class CTDChemGeneUploader(uploader.BaseSourceUploader):

    # main_source = "ConsensusPathDB"
    name = "CTD_chem_gene_ixns"
    collection_name = "CTD_chem_gene_ixns"
    zip_file_name = "CTD_chem_gene_ixns.tsv.gz"
    # __metadata__ = {"mapper": 'consensuspathdb_mapper'}

    def load_data(self, data_folder):
        downloaded_file = os.path.join(data_folder, self.zip_file_name)
        self.logger.info("Load data from file '%s'" % downloaded_file)
        return CTDChemGeneParser.parse_tsv_file(gzip.open(downloaded_file, mode='rt'))
