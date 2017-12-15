from biothings.utils.mongo import doc_feeder, get_target_db
from biothings.hub.databuild.builder import DataBuilder
from biothings.hub.dataload.storage import UpsertStorage

from hub.databuild.mapper import BiogridMapper


class InteractionDataBuilder(DataBuilder):

    def post_merge(self, source_names, batch_size, job_manager):
        # get the interaction mapper
        biogrid_mapper = BiogridMapper(name="biogrid_mapper")
        # load cache (it's being loaded automatically
        # as it's not part of an upload process
        biogrid_mapper.load()

        # create a storage to save docs back to merged collection
        db = get_target_db()
        col_name = self.target_backend.target_collection.name
        storage = UpsertStorage(db, col_name)

        for docs in doc_feeder(self.target_backend.target_collection, step=batch_size, inbatch=True):
            docs = biogrid_mapper.process(docs)
            storage.process(docs, batch_size)
