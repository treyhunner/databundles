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
    '''Represents a bundle partition, part of the bundle data broken out in 
    time, space, or by table. '''
    
    def __init__(self, bundle, partition_id, **kwargs):
        self.bundle = bundle
        self.pid= partition_id
        self.data = kwargs.get('data',{})
        self.state = kwargs.get('state', None)
      
    @property
    def name(self):
        parts = self.bundle.identity.name_parts(self.bundle.identity)
        
        np = parts[:-1]+[str(self.pid)]+parts[-1:]
        
        return  '-'.join(np)
    
    @property
    def database(self):
        from database import PartitionDb
        return PartitionDb(self.bundle, self)
 
    def __repr__(self):
        return "<partition: {}>".format(self.name)
 
class Partitions(object):
    '''Continer and manager for the set of partitions. '''
    
    def __init__(self, bundle):
        self.bundle = bundle
        
   
    
    def partition(self, arg):
        '''Get a local partition object from either a Partion ORM object, or
        a partition name
        
        Arguments:
        arg    -- a orm.Partition or Partition object. 
        
        
        '''
        
        from databundles.orm import Partition as OrmPartition
        
        if isinstance(arg,OrmPartition):
            orm_partition = arg
        elif isinstance(arg, str):
            s = self.bundle.database.session        
            orm_partition = s.query(OrmPartition).filter(OrmPartition.id_==arg ).one
            
        else:
            raise ValueError("Arg must be a Partition or")
        
        partition_id = orm_partition.as_partition_id()
        
        
        return Partition(self.bundle, partition_id)
      
    
    @property
    def count(self):
        from databundles.orm import Partition as OrmPartition
        
        s = self.bundle.database.session
        return s.query(OrmPartition).count()
    
    @property
    def query(self):
        from databundles.orm import Partition as OrmPartition
        
        s = self.bundle.database.session
        return s.query(OrmPartition)
    
    def find(self, time, space, table):
        from databundles.orm import Partition as OrmPartition
        q = self.query
        
        if time is not None:
            q = q.filter(OrmPartition.time==time)

        if space is not None:
            q = q.filter(OrmPartition.space==space)
    
        if table is not None:
            tr = self.bundle.schema.table(table)
            q = q.filter(OrmPartition.t_id==tr.id_)
        
        return q.one()
    
    def generate(self):
        from databundles.orm import Partition as OrmPartition, Table
    
        s = self.bundle.database.session
    
        s.query(OrmPartition).delete()

        seen={}
        for i in self.bundle.partitionGenerator(): 
            table = None

            if i.name in seen:
                continue;
            
            seen[i.name]=True

            p = OrmPartition(id = i.name,
                             space = i.pid.space,
                             time=i.pid.time,
                             d_id = self.bundle.identity.id_,
                             data=i.data,
                             state=i.state) 
            s.add(p)
            
            if i.pid.table:
                try:
                    table = s.query(Table).filter(Table.name==i.pid.table).one()
                except:
                    table = None
            else:
                table = None 
                
            if table is not None:
                p.t_id = table.id_
                  
        s.commit()
