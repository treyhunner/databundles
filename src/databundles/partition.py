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
        return '.'.join([re.sub('[^\w\.]','_',str(s))
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
    def path(self):
        import os.path
        parts = self.bundle.identity.name_parts(self.bundle.identity)
        source = parts.pop(0)
        p = self.pid
        pparts = [ i for i in [p.time,p.space,p.table] if i is not None]
        
        
        return  os.path.join(source, '-'.join(parts), *pparts )
    
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
        
        return Partition(self.bundle, partition_id, data=orm_partition.data, 
                      state = orm_partition.state)
    
    
    @property
    def count(self):
        from databundles.orm import Partition as OrmPartition
        
        s = self.bundle.database.session
        return s.query(OrmPartition).count()
    
    @property 
    def all(self):
        '''Return an iterator of all partitions'''
        from databundles.orm import Partition as OrmPartition
        s = self.bundle.database.session      
        return [self.partition(op) for op in s.query(OrmPartition).all()]
    
    @property
    def query(self):
        from databundles.orm import Partition as OrmPartition
        
        s = self.bundle.database.session
        return s.query(OrmPartition)
    
    def find_orm(self, pid=None, **kwargs):
        '''Return a Partition object from the database based on a PartitionId.
        An ORM object is returned, so changes can be persisted. '''
        import sqlalchemy.orm.exc
        
        if not pid: 
            time = kwargs.get('time',None)
            space = kwargs.get('space', None)
            table = kwargs.get('table', None)
        else:
            time = pid.time
            space = pid.space
            table = pid.table
                
        from databundles.orm import Partition as OrmPartition
        q = self.query
        
        if time is not None:
            q = q.filter(OrmPartition.time==time)

        if space is not None:
            q = q.filter(OrmPartition.space==space)
    
        if table is not None:
            tr = self.bundle.schema.table(table)
            q = q.filter(OrmPartition.t_id==tr.id_)

        try:
            return q.one()   
        except sqlalchemy.orm.exc.NoResultFound: 
            return None
    
    def find(self, pid=None, **kwargs):
        '''Return a Partition object from the database based on a PartitionId.
        The object returned is immutable; changes are not persisted'''
        op = self.find_orm(pid, **kwargs)
        
        if op is not None:
            return self.partition(op)
        else:
            return None
    
    
    table_cache = {}
    def new_orm_partition(self, pid, **kwargs):
        from databundles.orm import Partition as OrmPartition, Table
        

        if pid.table:
            if pid.table in self.table_cache:
                table = self.table_cache[pid.table]
            else:
                try:
                    s = self.bundle.database.session
                    table = s.query(Table).filter(Table.name==pid.table).one()
                    self.table_cache[pid.table] = table
                   
                except:
                    table = None
        else:
            table = None 
        
        pid.table = table.id_ if table else None
     
        p = Partition(self.bundle, pid) 
        
        op = OrmPartition(id = p.name,
             space = pid.space,
             time = pid.time,
             t_id = pid.table,
             d_id = self.bundle.identity.id_,
             data=kwargs.get('data',None),
             state=kwargs.get('state',None),)  

        return op


    def delete_all(self):
        from databundles.orm import Partition as OrmPartition
        s = self.bundle.database.session
        s.query(OrmPartition).delete()
        
    def new_partition(self, pid, **kwargs):
      
        p = self.find(pid)
        
        if p is not None:
            return p
      
        op = self.new_orm_partition(pid, **kwargs)
        s = self.bundle.database.session
        s.add(op)        
        s.commit()

        return self.partition(op)


    def generate(self):
        '''Call the partitionGenerator() method on a Bundle to get partitions'''
        from databundles.orm import Partition as OrmPartition
    
        s = self.bundle.database.session
    
        s.query(OrmPartition).delete()

        partitions = {}
        for i in self.bundle.partitionGenerator(): 
            p = self.new_orm_partition(i.pid,data=i.data, state=i.state)
     
            partitions[i.pid] = p
          
        for pid,p in partitions.items():
            s.add(p)        
            s.commit()
              

