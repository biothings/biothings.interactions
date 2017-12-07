import biothings, config
biothings.config_for_app(config)

import biothings.utils.mongo as mongo
import biothings.hub.databuild.mapper as mapper
from hub.dataload.sources.ConsensusPathDB import ConsensusPathDBUploader
from hub.dataload.sources.biogrid import BiogridUploader

import logging


class ConsensusPathDBMapper(mapper.BaseMapper):

    def __init__(self, *args, **kwargs):
        super(ConsensusPathDBMapper, self).__init__(*args, **kwargs)
        self.cache = None

    def load(self):
        if self.cache is None:
            # this is a whole dict containing all data

            # Note this Mapper is targeted at ConsensusPathDB
            # TODO:  point the mapper at other Uploader names
            col = mongo.get_src_db()[ConsensusPathDBUploader.name]
            self.cache = [d["_id"] for d in col.find({}, {"interaction_participants": 1})]

    def process(self, docs):
        for doc in docs:
            yield doc


class BiogridMapper(mapper.BaseMapper):

    def __init__(self, *args, **kwargs):
        super(BiogridMapper, self).__init__(*args, **kwargs)
        self.cache = None

    def load(self):
        if self.cache is None:
            # this is a whole dict containing all data

            # Note this Mapper is targeted at ConsensusPathDB
            # TODO:  point the mapper at other Uploader names
            col = mongo.get_src_db()[BiogridUploader.name]

            # Log the data structure
            for d in col.find({}, {"_id": 1}):
                logging.info(d.keys())

            self.cache = [d["_id"] for d in col.find({}, {"biogrid_interaction_id": 1})]

            # self.cache = [d["biogrid_interaction_id"] for d in col.find({}, {"biogrid_interaction_id": 1})]

    def process(self, docs):
        for doc in docs:
            yield doc
