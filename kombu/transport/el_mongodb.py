"""
kombu.transport.el_mongodb
=======================

ElMongoDB transport.

:copyright: (c) 2014 - Elastica
"""
# imports
import logging
from multiprocessing import Manager

# global data
manager = Manager()
GlobalData = manager.dict()
logger = logging.getLogger('kombu.transport.el_mongodb')


class ElMongodbChannel(object):

    """
    Fairness management for Elastica's active tenants
    """

    def __init__(self, mongodb_client, queue):

        self.mongodb_client = mongodb_client  # mongo client
        self.queue = queue  # queue being consumed

        logger.debug('Queue: "%s" | Tenants: "%s"' % (self.queue, ','.join(GlobalData.get(self.queue, []))))

        #----------------------------------------------------------------------------------------------
        # GlobalData is initially an empty dict
        # Keys of GlobalData will be queue names
        # Values of GlobalData will be list of active tenants for respective queues
        # So GlobalData would look like:
        # GlobalData = {'box_queue': ['elasticaco', 'elasticanet'],
        #               'box_dnld_queue': ['elasticanet', 'elasticala', 'elastica']}
        # This GlobalData shows that the worker is consuming two queues 'box_queue' and 'box_dnld_queue'
        #----------------------------------------------------------------------------------------------

        # Functionality of __init__ :
        #----------------------------------------------------------------------------------------------
        # If the given queue name does not exist in GlobalData's keys we will:
        #   -add this queue as a key in GlobalData
        #   -initialize active tenants list for this queue and set as the respective value of key

        # If an empty list is found for the given key in GlobalData, then we will:
        #   -re-initialize active tenants list for this queue
        #   -update the value for this queue in GlobalData
        #----------------------------------------------------------------------------------------------

        if not GlobalData.get(self.queue, []):
            GlobalData[self.queue] = self._load_active_tenants()
            logger.debug('(Re)Initialization - Queue: "%s" | Tenants: "%s"' % (self.queue, ','.join(GlobalData.get(self.queue, []))))

    def _load_active_tenants(self):
        '''
        Loads only the active tenants for the given queue
        '''

        active_tenants = []
        try:
            # Filter upon queue and get only teh distinct values of the field "tenant"
            # "distinct": Finds the distinct values for a specified field across a single collection
            active_tenants = list(
                self.mongodb_client.messages.find({"queue": self.queue}).distinct("tenant"))
            logger.debug("[SUCCESS]: ElMongodbChannel: load-active-tenants: Success!")

        except Exception:
            logger.error("[ERR]: ElMongodbChannel: load-active-tenants: Failure!")
        return active_tenants

    def get_tenant(self):
        """
        Selects the tenant to be honored
        """

        global_tenants = GlobalData.get(self.queue, [])
        if not global_tenants:
            return None

        # Pop out the tenant that is sitting at index zero, it will be honored
        tenant = global_tenants.pop(0)
        logger.debug("Processing task for [Queue: (%s) & Tenant: (%s)]" % (self.queue, tenant))

        # update the tenants list for the given queue in GlobalData
        GlobalData[self.queue] = global_tenants
        return tenant
