'''
Created on Jun 23, 2012

@author: eric
'''

import os

from identity import Identity

class PartitionIdentity(Identity):
    '''Subclass of Identity for partitions'''
    
    time = None
    space = None
    table = None
    
    def __init__(self, *args, **kwargs):

        d = {}

        for arg in args:
            if isinstance(arg, Identity):
                d = arg.to_dict()
       
    
        d = dict(d.items() + kwargs.items())
    
        self.from_dict(d)
        
        self.name # Trigger some errors immediately. 
            
    def from_dict(self,d):
        
        super(PartitionIdentity, self).from_dict(d)
        
        self.time = d.get('time',None)
        self.space = d.get('space',None)
        self.table = d.get('table',None)
        self.grain = d.get('grain',None)
        
        from identity import ObjectNumber
        if self.id_ is not None and self.id_[0] != ObjectNumber.TYPE.PARTITION:
            self.id_ = None

       
    def to_dict(self):
        '''Returns the identity as a dict. values that are empty are removed'''
        
        d =  super(PartitionIdentity, self).to_dict()
        
        d['time'] = self.time
        d['space'] = self.space
        d['table'] = self.table
        d['grain'] = self.grain

        return { k:v for k,v in d.items() if v}
    
    
    @classmethod
    def path_str(cls,o=None):
        '''Return the path name for this bundle'''
        import re
        
        id_path = Identity.path_str(o)

        partition_parts = [re.sub('[^\w\.]','_',str(s))
                         for s in filter(None, [o.time, o.space, o.table, o.grain])]
    
       
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
                         for s in filter(None, [o.time, o.space, o.table, o.grain])])
        
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
        
        self._schema = None
        self._database = None

        # The value for the library is injected in LocalLibrary.get() so the
        # partition can figure out what the pat to its database is. 
        self.library = None
      
    def init(self):
        '''Initialize the partition, loading in any SQL, etc. '''
        if not self.database.exists():
            self.database.create()

    @property
    def name(self):
        return self.pid.name
    
    @property
    def path(self):
        '''Return a pathname for the partition, relative to the containing 
        directory of the bundle. '''
        import os.path
        
        parts = self.bundle.identity.name_parts(self.bundle.identity)
       
        source = parts.pop(0)
        p = self.pid
        pparts = [ str(i) for i in [p.time,p.space,p.table] if i is not None]
       
        return  os.path.join(source, '-'.join(parts), *pparts )
    
    @property
    def database(self):
        if self._database is None:
            self._database = self._get_database()
            
        return self._database
    
    def _get_database(self):
        from database import PartitionDb
        
        # If the library is set, the path to the database is relative to the
        # library, not to the dataset
        if self.library is not None:
           
            return PartitionDb(self.bundle, self, file_path=self.library.path(self.path)+".db")
        else:
            db =  PartitionDb(self.bundle, self)
            db.file_path = None
            return db
    
 
    def __repr__(self):
        return "<partition: {}>".format(self.name)
 
    @property
    def identity(self):
        return self.pid
    
    @property
    def schema(self):
        from schema import Schema
        if self._schema is None:
            self._schema = Schema(self)
            
        return self._schema
    
    @property
    def table(self):
        '''Return the orm table for this partition, or None if
        no table is specified. 
        '''
        
        table_spec = self.identity.table
        
        if table_spec is None:
            return None
        
        return self.bundle.schema.table(table_spec)
        
        
    def create_with_tables(self, tables=None, clean=True):
        '''Create, or re-create,  the partition, possibly copying tables
        from the main bundle
        
        Args:
            tables. String or Array of Strings. Specifies the names of tables to 
            copy from the main bundle. 
            
            clean. If True, delete the database first. Defaults to true. 
        
        '''
        
        if clean:
            self.database.delete()
        
        self.database.create(copy_tables = False)
        
        if tables is not None:
        
            if not isinstance(tables, list):
                tables = [tables]
        
            for table in tables:         
                self.database.copy_table_from(self.bundle.database,table)
        elif self.table:
            self.database.copy_table_from(self.bundle.database,self.table.name)
        
        self.schema.create_tables()
        
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
        
        partition_id = orm_partition.identity
     
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
            grain = kwargs.get('grain', None)
        else:
            time = pid.time
            space = pid.space
            table = pid.table
            grain = pid.grain
                
        from databundles.orm import Partition as OrmPartition
        q = self.query
        
        if time is not None:
            q = q.filter(OrmPartition.time==time)

        if space is not None:
            q = q.filter(OrmPartition.space==space)
    
        if grain is not None:
            q = q.filter(OrmPartition.grain==grain)
    
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
   
    def new_orm_partition(self, pid, **kwargs):
        '''Create a new ORM Partrition object, or return one if
        it already exists '''
        from databundles.orm import Partition as OrmPartition, Table

        s = self.bundle.database.session
   
        if pid.table:
            q =s.query(Table).filter( (Table.name==pid.table) |  (Table.id_==pid.table) )
            table = q.one()
        else:
            table = None
         
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
        s.commit()     
       
        p = self.partition(op)
        return p


              

