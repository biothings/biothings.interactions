import biothings, config
biothings.config_for_app(config)

import biothings.utils.mongo as mongo
import biothings.hub.databuild.mapper as mapper
from hub.dataload.sources.ConsensusPathDB import ConsensusPathDBUploader


class InteractionMapper(mapper.BaseMapper):

    def __init__(self, *args, **kwargs):
        super(InteractionMapper, self).__init__(*args, **kwargs)
        self.cache = None

    def load(self):
        if self.cache is None:
            # this is a whole dict containing all data

            # Note this Mapper is targeted at ConsensusPathDB
            # TODO:  point the mapper at other Uploader names
            col = mongo.get_src_db()[ConsensusPathDBUploader.name]
            self.cache = [d["_id"] for d in col.find({}, {"_id": 1})]

    def process(self, docs):
        for doc in docs:
            yield doc
