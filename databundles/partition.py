"""Access classess and identity for partitions. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os

from databundles.identity import PartitionIdentity
from sqlalchemy.orm.exc import NoResultFound

        
class Partition(object):
    '''Represents a bundle partition, part of the bundle data broken out in 
    time, space, or by table. '''
    
    def __init__(self, bundle, record):
        self.bundle = bundle
        self.record = record
        
        self._database =  None
        self._hd5file = None
        self._tempfile_cache = {}
     
    def init(self):
        '''Initialize the partition, loading in any SQL, etc. '''
    
    @property
    def name(self):
        return self.identity.name
    
    @property
    def identity(self):
        return self.record.identity
    
    def _path_parts(self):

        name_parts = self.bundle.identity.name_parts(self.bundle.identity)
       
        source =  name_parts.pop(0)
        p = self.identity
        partition_path = [ str(i) for i in [p.table,p.time,p.space,p.grain] if i is not None]
       
        return source,  name_parts, partition_path 
    
    @property
    def path(self):
        '''Return a pathname for the partition, relative to the containing 
        directory of the bundle. '''
        source,  name_parts, partition_path = self._path_parts()
        
        return  os.path.join(source, '-'.join( name_parts), *partition_path )
        
    @property
    def database(self):
        if self._database is None:
            from databundles.database import PartitionDb
            
            source,  name_parts, partition_path = self._path_parts() #@UnusedVariable
            
            path = os.path.join(self.bundle.database.root_path, *partition_path)+".db"

            self._database = PartitionDb(self.bundle, self, file_path=path)
            
            def add_type(database):
                from databundles.bundle import BundleDbConfig
                config = BundleDbConfig(self.database)
                config.set_value('info','type','partition')
                
            self._database._post_create = add_type 
          
        return self._database

    def tempfile(self, table=None, suffix=None,ignore_first=False):
        '''Return a tempfile object for this partition'''
        
        ckey = (table,suffix)

        tf = self._tempfile_cache.get(ckey, None)   
        if tf:
            return tf
        else:                
            if table is None and self.table:
                table = self.table;
            tf = self.database.tempfile(table, suffix=suffix, ignore_first=ignore_first)
            self._tempfile_cache[ckey] = tf
            return tf
      
    @property
    def hdf5file(self):
        from  databundles.hdf5 import Hdf5File
        if self._hd5file is None:
            self._hd5file = Hdf5File(self)
            
        return self._hd5file

    @property
    def data(self):
        return self.record.data
    
    
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
            tables = [self.table.name]
     
        for t in tables:
            if not t in self.database.inspector.get_table_names():
                t_meta, table = self.bundle.schema.get_table_meta(t) #@UnusedVariable
                t_meta.create_all(bind=self.database.engine)
    

    def create(self):
        self.create_with_tables(tables=self.identity.table)


    @property
    def extents(self, where=None):
        '''Return the bounding box for the dataset. The partition must specify 
        a table
        
        '''
        import geo.util
        return geo.util.extents(self.database,self.table.name, where=where)
        

    def __repr__(self):
        return "<partition: {}>".format(self.name)

class GeoPartition(Partition):
    '''A Partition that hosts a Spatialite for geographic data'''
    

    def __init__(self, bundle, record):
        super(GeoPartition, self).__init__(bundle, record)

    def convert(self, table_name, progress_f=None):
        """Convert a spatialite geopartition to a regular partition
        by extracting the geometry and re-projecting it to WGS84
        
        :param config: a `RunConfig` object
        :rtype: a `LibraryDb` object
        
        :param config: a `RunConfig` object
        :rtype: a `LibraryDb` object
                
        """
        import subprocess, csv
        from databundles.orm import Column
        from databundles.dbexceptions import ConfigurationError
        
        command_template = """spatialite -csv -header {file} "select *,   
        X(Transform(geometry, 4326)) as _db_lon, Y(Transform(geometry, 4326)) 
        as _db_lat from {table}" """  
        
        
        #
        # Duplicate the geo partition table for the new partition
        # Then make the new partition
        #
        
        
        t = self.bundle.schema.add_table(table_name)
        
        ot = self.table
        
        for c in ot.columns:
                self.bundle.schema.add_column(t,c.name,datatype=c.datatype)
                
        self.bundle.schema.add_column(t,'_db_lon',datatype=Column.DATATYPE_REAL)
        self.bundle.schema.add_column(t,'_db_lat',datatype=Column.DATATYPE_REAL)
        self.bundle.database.commit()

        pid = self.identity
        pid.table = table_name
        partition = self.bundle.partitions.new_partition(pid)
        partition.create_with_tables()
        
        #
        # Open a connection to spatialite and run the query to 
        # extract CSV. 
        #
        # It would be a lot more efficient to connect to the 
        # Spatialite procss, attach the new database, the copt the 
        # records in SQL. 
        #
        
        try:
            subprocess.check_output('spatialite -version', shell=True)
        except:
            raise ConfigurationError('Did not find spatialite on path. Install spatialite')
        
        command = command_template.format(file=self.database.path,
                                          table = self.identity.table)
        
        #self.bundle.log("Running: {}".format(command))
        
        p = subprocess.Popen(command, stdout = subprocess.PIPE, shell=True)
        stdout, stderr = p.communicate()
        
        #
        # Finally we can copy the data. 
        #
        
        reader = csv.reader(stdout.decode('ascii').splitlines())
        header = reader.next()
       
        if not progress_f:
            progress_f = lambda x: x
       
        with partition.database.inserter(table_name) as ins:
            for i, line in enumerate(reader):
                ins.insert(line)
                progress_f(i)
                
                


class Partitions(object):
    '''Continer and manager for the set of partitions. 
    
    This object is always accessed from Bundle.partitions""
    '''
    
    def __init__(self, bundle):
        self.bundle = bundle

    def partition(self, arg, is_geo=False):
        '''Get a local partition object from either a Partion ORM object, or
        a partition name
        
        Arguments:
        arg    -- a orm.Partition or Partition object. 
        
        '''

        from databundles.orm import Partition as OrmPartition
        from databundles.identity import PartitionNumber
        
        if isinstance(arg,OrmPartition):
            orm_partition = arg
        elif isinstance(arg, str):
            s = self.bundle.database.session        
            orm_partition = s.query(OrmPartition).filter(OrmPartition.id_==arg ).one()
        elif isinstance(arg, PartitionNumber):
            s = self.bundle.database.session        
            orm_partition = s.query(OrmPartition).filter(OrmPartition.id_==str(arg) ).one()
        else:
            raise ValueError("Arg must be a Partition or PartitionNumber")

        if orm_partition.data.get('is_geo'):
            is_geo = True
        elif is_geo: # The caller signalled that this should be a Geo, but it isn't so set it. 
            orm_partition.data['is_geo'] = True
            s = self.bundle.database.session    
            s.merge(orm_partition)
            s.commit()
        
        partition_id = orm_partition.identity #@UnusedVariable


        if is_geo:
            return GeoPartition(self.bundle, orm_partition)
        else:
            return Partition(self.bundle, orm_partition)

    @property
    def count(self):
        from databundles.orm import Partition as OrmPartition
        
        s = self.bundle.database.session
        return s.query(OrmPartition).count()
    
    @property 
    def all(self): #@ReservedAssignment
        '''Return an iterator of all partitions'''
        from databundles.orm import Partition as OrmPartition
        import sqlalchemy.exc
        try:
            s = self.bundle.database.session      
            return [self.partition(op) for op in s.query(OrmPartition).all()]
        except sqlalchemy.exc.OperationalError:
            return []
            
        
    def __iter__(self):
        return iter(self.all)

            
    @property
    def query(self):
        from databundles.orm import Partition as OrmPartition
        
        s = self.bundle.database.session
        return s.query(OrmPartition)
 
    
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
        
        if isinstance(id_, PartitionIdentity):
            id_ = id_.identity.id_
            
        
        q = (self.bundle.database.session
             .query(OrmPartition)
             .filter(OrmPartition.id_==id_.encode('ascii')))
      
        try:
            orm_partition = q.one()
          
            return self.partition(orm_partition)
        except NoResultFound:
            orm_partition = None
            
        if not orm_partition:
            q = (self.bundle.database.session
             .query(OrmPartition)
             .filter(OrmPartition.name==id_.encode('ascii')))
            
            try:
                orm_partition = q.one()
              
                return self.partition(orm_partition)
            except NoResultFound:
                orm_partition = None
            
        return orm_partition

    def find_table(self, table_name):
        '''Return the first partition that has the given table name'''
        
        for partition in self.all:
            if partition.table and partition.table.name == table_name:
                return partition
            
        return None

    def find(self, pid=None, **kwargs):
        '''Return a Partition object from the database based on a PartitionId.
        The object returned is immutable; changes are not persisted'''
        op = self.find_orm(pid, **kwargs)
        
        if op is not None:
            return self.partition(op)
        else:
            return None
    
    def find_orm(self, pid=None, **kwargs):
        '''Return a Partition object from the database based on a PartitionId.
        An ORM object is returned, so changes can be persisted. '''
        import sqlalchemy.orm.exc
        from databundles.identity import Identity
        
        if not pid: 
            time = kwargs.get('time',None)
            space = kwargs.get('space', None)
            table = kwargs.get('table', None)
            grain = kwargs.get('grain', None)
            name = kwargs.get('name', None)
        elif isinstance(pid, Identity):
            time = pid.time
            space = pid.space
            table = pid.table
            grain = pid.grain
            name = None
        elif isinstance(pid,basestring):
            time = None
            space = None
            table = None
            grain = None
            name = pid            
        
                
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
            
            if not tr:
                return None
                #raise ValueError("Didn't find table named {} ".format(table))
            
            q = q.filter(OrmPartition.t_id==tr.id_)

        if name is not None:
            q = q.filter(OrmPartition.name==name)

        try:
            return q.one()   
        except sqlalchemy.orm.exc.NoResultFound: 
            return None
    
   
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
             grain = pid.grain, 
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
       
        p = self.partition(op, is_geo=kwargs.get('is_geo',None))
        return p

    def new_geo_partition(self, pid, shape_file):
        """Load a shape file into a partition as a spatialite database. 
        
        Will also create a schema entry for the table speficified in the 
        table parameter of the  pid, using the fields from the table in the
        shapefile
        """
        import subprocess, sys
        import shapefile, tempfile, uuid
        from databundles.database import Database
        from databundles.dbexceptions import ConfigurationError
        
        try:
            subprocess.check_output('ogr2ogr --help-general', shell=True)
        except:
            raise ConfigurationError('Did not find ogr2ogr on path. Install gdal/ogr')
        
        ogr_create="ogr2ogr  -f SQLite {output} -nln \"{table}\" {input}  -dsco SPATIALITE=yes"
        
        if not pid.table:
            raise ValueError("Pid must have a table name")
         
        table_name = pid.table
        
        t = self.bundle.schema.add_table(pid.table)
        self.bundle.database.commit()
        
        partition = self.new_partition(pid, is_geo=True)
        
        dir_ = os.path.dirname(partition.database.path)
        if not os.path.exists(dir_):
            self.bundle.log("Make dir_ "+dir_)
            os.makedirs(dir_)
        
        cmd = ogr_create.format(input = shape_file,
                                output = partition.database.path,
                                table = table_name )
        
        self.bundle.log("Running: "+ cmd)
    
        output = subprocess.check_output(cmd, shell=True)

        for row in partition.database.connection.execute("pragma table_info('{}')".format(table_name)):
            self.bundle.schema.add_column(t,row[1],datatype = row[2].lower())

        return partition


    def find_or_new(self, pid, **kwargs):

        partition =  self.find(pid)
        
        if not partition:
            partition = self.new_partition(pid, **kwargs)
        
        return partition;
    
    def delete(self, partition):
        from databundles.orm import Partition as OrmPartition

        q = (self.bundle.database.session
             .query(OrmPartition)
             .filter(OrmPartition.id_==partition.identity.id_))
      
        q.delete()
  
    
              

