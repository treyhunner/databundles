'''
Created on Jun 23, 2012

@author: eric
'''

import os

from identity import Identity

class PartitionIdentity(Identity):
    '''Has a similar interface to Identity'''
    
    time = None
    space = None
    table = None
    
    def __init__(self, *args, **kwargs):

        got_id = False
        for arg in args:
            if isinstance(arg, Identity):
                self.from_dict(arg.to_dict())
                got_id = True
    
        if not got_id:
            super(PartitionIdentity, self).__init__(**kwargs)
    
        self.time = kwargs.get('time',None)
        self.space = kwargs.get('space',None)
        self.table = kwargs.get('table',None)
    
        from objectnumber import ObjectNumber
        if self.id_ is not None and self.id_[0] != ObjectNumber.TYPE.PARTITION:
            self.id_ = None
            
    def from_dict(self,d):
        
        return super(PartitionIdentity, self).from_dict(d)
        
        self.time = d.get('time',None)
        self.space = d.get('space',None)
        self.table = d.get('table',None)
        
        from objectnumber import ObjectNumber
        if self.id_ is not None and self.id_[0] != ObjectNumber.TYPE.PARTITION:
            self.id_ = None
        

    def to_dict(self):
        '''Returns the identity as a dict. values that are empty are removed'''
        
        d =  super(PartitionIdentity, self).to_dict()
        
        d['time'] = self.time
        d['space'] = self.space
        d['table'] = self.table

        return { k:v for k,v in d.items() if v}
    
    
    @classmethod
    def path_str(cls,o=None):
        '''Return the path name for this bundle'''
        import re
        
        id_path = Identity.path_str(o)

        partition_parts = [re.sub('[^\w\.]','_',str(s))
                         for s in filter(None, [o.time, o.space, 
                                                o.table])]
    
       
        return  os.path.join(id_path ,  *partition_parts )
        
    
    @classmethod
    def name_str(cls,o=None):
        
        return '-'.join(cls.name_parts(o))
    
    @classmethod
    def name_parts(cls,o=None):
        import re
       
        parts = Identity.name_parts(o)
    
        rev = parts.pop()
        
        partition_component = '.'.join([re.sub('[^\w\.]','_',str(s))
                         for s in filter(None, [o.time, o.space, 
                                                o.table])])
        
        parts.append(partition_component)
        parts.append(rev)
        
        return parts
    

class Partition(object):
    '''Represents a bundle partition, part of the bundle data broken out in 
    time, space, or by table. '''
    
    def __init__(self, bundle, partition_id, **kwargs):
        self.bundle = bundle
        self.pid= partition_id
        self.data = kwargs.get('data',{})
        self.state = kwargs.get('state', None)
        
        self.pid.id_ = kwargs.get('id',kwargs.get('id_',None))
      
        self.pid.partition = self

      
    def init(self):
        '''Initialize the partition, loading in any SQL, etc. '''
        if not self.database.exists():
            self.database.create()

  
    @property
    def name(self):
        return self.pid.name
    
    @property
    def path(self):
        import os.path
        parts = self.bundle.identity.name_parts(self.bundle.identity)
       
        source = parts.pop(0)
        p = self.pid
        pparts = [ str(i) for i in [p.time,p.space,p.table] if i is not None]
       
        return  os.path.join(source, '-'.join(parts), *pparts )
    
    @property
    def database(self):
        from database import PartitionDb
        return PartitionDb(self.bundle, self)
    
 
    def __repr__(self):
        return "<partition: {}>".format(self.name)
 
    @property
    def identity(self):
        return self.pid
        
        
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
        

        return Partition(self.bundle, 
                         partition_id, 
                         id=orm_partition.id_,
                         data=orm_partition.data, 
                         state = orm_partition.state)
    
    
    @property
    def count(self):
        from databundles.orm import Partition as OrmPartition
        
        s = self.bundle.database.session
        return s.query(OrmPartition).count()
    
    @property 
    def all(self): #@ReservedAssignment
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
    
    
    def get(self, id_):
        '''Get a partition by the id number 
        
        Arguments:
            id_ -- a partition id value
            
        Returns:
            A partitions.Partition object
            
        Throws:
            a Sqlalchemy exception if the partition either does not exist or
            is not unique
        ''' 
        from databundles.orm import Partition as OrmPartition
        
        # This is needed to flush newly created partitions, I think ... 
        self.bundle.database.session.close()
        
        q = (self.bundle.database.session
             .query(OrmPartition)
             .filter(OrmPartition.id_==id_.encode('ascii')))
      
        return self.partition(q.one())
    
    table_cache = None
    def new_orm_partition(self, pid, **kwargs):
        from databundles.orm import Partition as OrmPartition, Table

        if self.table_cache is None:
            self.table_cache = {}
            
        s = self.bundle.database.session
        for table in s.query(Table).filter(Table.name==pid.table).all():
            self.table_cache[table.name] = table
            self.table_cache[table.id_] = table
                
        table = None
        if pid.table:
            if pid.table in self.table_cache:
                table = self.table_cache[pid.table]
        
        op = OrmPartition(name = pid.name,
             space = pid.space,
             time = pid.time,
             t_id = table.id_ if table else None,
             d_id = self.bundle.identity.id_,
             data=kwargs.get('data',None),
             state=kwargs.get('state',None),)  

        return op


    def clean(self):
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
       
        p = self.partition(op)
    
        return p


              

