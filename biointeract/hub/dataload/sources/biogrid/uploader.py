import os
from zipfile import ZipFile

import biothings, config_hub
biothings.config_for_app(config_hub)

import biothings.hub.dataload.uploader as uploader
from .parser import BiogridParser


class BiogridUploader(uploader.BaseSourceUploader):

    # main_source = "biogrid"
    name = "biogrid"
    collection_name = "biogrid"

    # file information
    zip_file_name = "BIOGRID-ALL-3.4.156.tab2.zip"
    tab_file = "BIOGRID-ALL-3.4.156.tab2.txt"
    # __metadata__ = {"mapper": 'biogrid_mapper'}

    def load_data(self, data_folder):
        consensus_file = os.path.join(data_folder, self.zip_file_name)

        self.logger.info("Load biogrid data from file '%s'" % consensus_file)

        # Open the first file in the zip file - assuming that it is the data file
        zip = ZipFile(consensus_file)
        tab_file = zip.open(zip.namelist()[0], mode='r')

        return BiogridParser.parse_biogrid_tsv_file(tab_file)
