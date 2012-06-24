'''
Created on Jun 23, 2012

@author: eric
'''

class PartitionId(object):

    def __init__(self, **kwargs):
     
        self.time = kwargs.get('time',None)
        self.space = kwargs.get('space',None)
        self.table = kwargs.get('table',None)

    def __str__(self):
        '''Return the parttion component of the name'''
        import re
        return '.'.join([re.sub('[^\w\.]','_',s).lower() 
                         for s in filter(None, [self.time, self.space, 
                                                self.table])])

class Partition(object):
    '''Represents a bundle partition, part of the bunle data broken out in 
    time, space, or by table. '''
    
    def __init__(self, bundle, partition_id):
        self.bundle = bundle
        self.pid= partition_id
      
    @property
    def name(self):
        parts = self.bundle.identity.name_parts()
        
        np = parts[:-1]+[str(self.pid)]+parts[-1:]
        
        return  '-'.join(np)
    
    @property
    def database(self):
        pass
 
class Partitions(object):
    '''Continer and manager for the set of partitions. '''
    
    def __init__(self, bundle):
        self.bundle = bundle
        
    def new_parition(self, **kwargs):
       
        return Partition(self.bundle,  PartitionId(**kwargs))
    
        
    def partition(self, partition_id):
        
        if not self._partition:
            p = self.config.get('partition',
                                {'time':None, 'state': None, 'table': None})
            self._partition=Partition(self,p.get('time', None),
                                      p.get('space', None),p.get('table', None))
       
        return self._partition
    
    def generate(self):
        from databundles.orm import Partition
      
        s = self.bundle.database.session

        s.query(Partition).delete()
      

        for i in self.bundle.partitionGenerator():       
            print i.name
        
        s.commit()
