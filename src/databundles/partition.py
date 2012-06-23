'''
Created on Jun 23, 2012

@author: eric
'''


class PartitionId(object):

    def __init__(self, bundle, time=None, space=None, table=None):
        self.bundle = bundle
        self.time = time
        self.space = space
        self.table = table

    def __str__(self):
        '''Return the parttion component of the name'''
        import re
        return '.'.join([re.sub('[^\w\.]','_',s).lower() 
                         for s in filter(None, [self.pid.time, self.pid.space, 
                                                self.pid.table])])


class Partition(object):
    '''Represents a bundle partition, part of the bunle data broken out in 
    time, space, or by table. '''
    
  
    def __init__(self, bundle, partition_id):
        self.bundle = bundle
        self.pid= partition_id
      
    @property
    def name(self):
        pass
    
    @property
    def database(self):
        pass
 
