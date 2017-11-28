from biothings.utils.mongo import doc_feeder, get_target_db
from biothings.hub.databuild.builder import DataBuilder
from biothings.hub.dataload.storage import UpsertStorage

from hub.databuild.mapper import InteractionMapper


class InteractionDataBuilder(DataBuilder):

    def post_merge(self, source_names, batch_size, job_manager):
        # get the biogrid mapper
        mapper = InteractionMapper(name="ConsensusPathDB")
        # load cache (it's being loaded automatically
        # as it's not part of an upload process
        mapper.load()

        # create a storage to save docs back to merged collection
        db = get_target_db()
        col_name = self.target_backend.target_collection.name
        storage = UpsertStorage(db, col_name)

        for docs in doc_feeder(self.target_backend.target_collection, step=batch_size, inbatch=True):
            docs = mapper.process(docs)
            storage.process(docs,batch_size)

        # add indices used to create metadata stats
        keys = ["rank", "taxid"]
        self.logger.info("Creating indices on %s" % repr(keys))
        for k in keys:
            self.target_backend.target_collection.ensure_index(k)

    def get_metadata(self, sources, job_manager):
        self.logger.info("Computing metadata...")
        # we want to compute it from scratch
        meta = {"__REPLACE__": True}
        self.logger.info("Metadata: %s" % meta)
        return meta
