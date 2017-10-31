# -*- coding: utf-8 -*-
from biothings.web.api.es.transform import ESResultTransformer
from enum import Enum
import logging

class ESResultTransformer(ESResultTransformer):

    # *****************************************************************************
    # Enumerated class for ConsensusPathDB PPI field names
    # *****************************************************************************
    class PpiFields(object):
        CONF = 'interaction_confidence'
        PART = 'interaction_participants'
        PUBS = 'interaction_publications'
        DBS = 'source_databases'

    # *****************************************************************************
    # Safe parse for interaction_confidence
    # *****************************************************************************
    @staticmethod
    def parse_confidence(res, i, hit):
        if ESResultTransformer.PpiFields.CONF in hit['_source']:
            val = hit['_source'][ESResultTransformer.PpiFields.CONF]
            val = None if (val == "NA" or val == '') else float(val)
            res['hits']['hits'][i]['_source'][ESResultTransformer.PpiFields.CONF] = val
        else:
            logging.warning("Field interaction_confidence not returned for this entry.".format(hit['_source']))
            res['hits']['hits'][i]['_source'][ESResultTransformer.PpiFields.CONF] = None
        return res

    # *****************************************************************************
    # Safe parse for interaction_participants
    # *****************************************************************************
    @staticmethod
    def parse_participants(res, i, hit):
        # participants - list of strings
        if ESResultTransformer.PpiFields.PART in hit['_source']:
            val = hit['_source'][ESResultTransformer.PpiFields.PART].replace("_HUMAN", "")
            res['hits']['hits'][i]['_source'][ESResultTransformer.PpiFields.PART] = [val] if ',' not in val else val.split(',')
        else:
            logging.warning("Field interaction_participants not returned for this entry - {}".format(hit['_source']))
            res['hits']['hits'][i]['_source'][ESResultTransformer.PpiFields.PART] = []
        return res

    # *****************************************************************************
    # Safe parse for interaction_publications
    # *****************************************************************************
    @staticmethod
    def parse_publications(res, i, hit):
        # publications - list of integers
        if ESResultTransformer.PpiFields.PUBS in hit['_source']:
            val = hit['_source'][ESResultTransformer.PpiFields.PUBS]
            pubs_strs = [val] if ',' not in val else val.split(',')
            try:
                pubs_ints = list(map(int, pubs_strs))
            except ValueError:
                pubs_ints = []
                logging.warning("Publication Entry for this query is not valid - {}".format(pubs_strs))
            res['hits']['hits'][i]['_source'][ESResultTransformer.PpiFields.PUBS] = pubs_ints
        else:
            logging.warning("Field interaction_publications not returned for this entry - {}".format(hit['_source']))
            res['hits']['hits'][i]['_source'][ESResultTransformer.PpiFields.PUBS] = []
        return res

    # *****************************************************************************
    # Safe parse for source_databases
    # *****************************************************************************
    @staticmethod
    def parse_databases(res, i, hit):
        # databases - list of strings
        if ESResultTransformer.PpiFields.DBS in hit['_source']:
            val = hit['_source'][ESResultTransformer.PpiFields.DBS]
            res['hits']['hits'][i]['_source'][ESResultTransformer.PpiFields.DBS] = [val] if ',' not in val else val.split(',')
        else:
            logging.warning("Field source_databases not returned for this entry - {}".format(hit['_source']))
            res['hits']['hits'][i]['_source'][ESResultTransformer.PpiFields.DBS] = []
        return res

    # #############################################################################
    # Add Human_PPI specific result transformations for query results
    # #############################################################################
    def clean_query_GET_response(self, res):
        logging.info("Query transformation call on {} hits".format(len(res['hits']['hits'])))

        for i, hit in enumerate(res['hits']['hits']):
            res = self.parse_confidence(res, i, hit)
            res = self.parse_participants(res, i, hit)
            res = self.parse_publications(res, i, hit)
            res = self.parse_databases(res, i, hit)
        return res
