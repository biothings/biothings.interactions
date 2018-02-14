import os
from zipfile import ZipFile

import biothings, config_hub
biothings.config_for_app(config_hub)

import biothings.hub.dataload.uploader as uploader
from .parser import HiNTParser


class HiNTUploader(uploader.BaseSourceUploader):

    name = "hint"
    collection_name = "hint"
    tab_file = "HomoSapiens_htb_hq.txt"
    # __metadata__ = {"mapper": 'hint_mapper'}

    def load_data(self, data_folder):
        file = os.path.join(data_folder, self.tab_file)
        return HiNTParser.parse_tsv_file(open(file, 'r'))
