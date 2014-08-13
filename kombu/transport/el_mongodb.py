"""
kombu.transport.el_mongodb
=======================

ElMongoDB transport.

:copyright: (c) 2014 - Elastica
"""
# imports
from collections import deque
from multiprocessing import Manager

# global data
manager = Manager()
GlobalData = manager.dict()
GlobalData['RotatingTenants'] = None
GlobalData['counter'] = 1


class ElMongodbChannel(object):

    """
    Fairness management for Elastica's active tenants
    """

    def __init__(self, mongodb_client, queue):

        self.mongodb_client = mongodb_client  # mongo client
        self.queue = queue  # queue being consumed
        self.active_tenants = []  # Active tenants default count is zero

        # fresh start...
        if not GlobalData['RotatingTenants'] and GlobalData["counter"] == 1:
            print "GlobalData Before Initialization: ", GlobalData
            self.load_global_tenants()
            print "GlobalData After Initialization: ", GlobalData

    def load_active_tenants(self):
        '''
        Loads only the active tenants
        '''
        try:
            self.active_tenants = list(
                self.mongodb_client.messages.find({"queue": self.queue}).distinct("tenant"))
            print "[SUCCESS]: ElMongodbChannel: load-active-tenants: Success!"
            return True
        except Exception:
            print "[ERR]: ElMongodbChannel: load-active-tenants: Failure!"
            return False

    def load_global_tenants(self):
        """
        Loads tenants to be rotated in this cycle
        """
        self.load_active_tenants()
        GlobalData['RotatingTenants'] = deque(self.active_tenants)

    def rotate_tenants(self):
        """
        Rotates tenant's list to ensure fairness
        """

        rotator = GlobalData['RotatingTenants']
        rotator.rotate(-1)
        GlobalData['RotatingTenants'] = rotator

        counter = GlobalData["counter"]
        if counter == len(rotator):
            GlobalData['counter'] = 1
            GlobalData['RotatingTenants'] = None
        else:
            counter += 1
            GlobalData['counter'] = counter

        return

    def get_tenant(self):
        """
        Selects the tenant whose task will be processed currently
        """

        if GlobalData['RotatingTenants']:

            print "GlobalData Before Rotation: ", GlobalData

            tenant = GlobalData['RotatingTenants'][0]
            print "Honourable Tenant: ", tenant

            self.rotate_tenants()
            print "GlobalData After Rotation: ", GlobalData
            return tenant
        else:
            return None
