"""A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it. 
"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

from databundles.run import  get_runconfig

import os.path

from databundles.dbexceptions import ConfigurationError
from databundles.filesystem import  Filesystem
from  databundles.identity import new_identity
from databundles.bundle import DbBundle
        
import databundles

from collections import namedtuple
from sqlalchemy.exc import IntegrityError

libraries = {}

# Setup a default logger. The logger is re-assigned by the
# bundle when the bundle instantiates the logger. 
import logging #@UnusedImport
import logging.handlers #@UnusedImport

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

    
def get_database(config=None,name='library'):
    """Return a new `LibraryDb`, constructed from a configuration
    
    :param config: a `RunConfig` object
    :rtype: a `LibraryDb` object
    
    If config is None, the function will constuct a new RunConfig() with a default
    constructor. 
    
    """
    import tempfile 
    
    if config is None:
        config = get_runconfig()    

    if not config.library:
        raise ConfigurationError("Didn't get library configuration value")
    
    root_dir = config.filesystem.get('root_dir',tempfile.gettempdir())
    db_config = config.database.get(name)
    
    db_config.dbname = db_config.dbname.format(root=root_dir)
    
    if not db_config:
        raise ConfigurationError("Didn't get database.{} configuration value".format(name))
    
    database = LibraryDb(**db_config)      
    database.create() # creates if does not exist. 
    
    return database


def _get_library(config=None, name='default'):
    
    if name is None:
        name = 'default'
    
    if config is None:
        config = get_runconfig()
    
    sc = config.library.get(name,False)
    
    if not sc:
        raise Exception("Failed to get library.{} config key ".format(name))
    
    filesystem = Filesystem(config)
    cache = filesystem.get_cache(sc.filesystem, config)
    
    database = get_database(config, name=sc.database)
    
    remote = sc.get('remote',None)
    
    l =  Library(cache = cache,  
                               database = database,
                               remote = remote)
    
    return l
    
def get_library(config=None, name='default', reset=False):
    """Return a new `Library`, constructed from a configuration
    
    :param config: a `RunConfig` object
    :rtype: a `Library` object
    
    If config is None, the function will constuct a new RunConfig() with a default
    constructor. 
    
    """    

    global libraries
    
    if reset:
        libraries = {}
    
    if name is None:
        name = 'default'

    if name not in libraries:
  
        libraries[name] = _get_library(config, name)
    
    return libraries[name]

def copy_stream_to_file(stream, file_path):
    '''Copy an open file-list object to a file
    
    :param stream: stream to copy from 
    :param file_path: file to write to. Will be opened mode 'w'
    
    '''

    with open(file_path,'w') as f:
        chunksize = 8192
        chunk =  stream.read(chunksize) #@UndefinedVariable
        while chunk:
            f.write(chunk)
            chunk =  stream.read(chunksize) #@UndefinedVariable

class LibraryDb(object):
    '''Represents the Sqlite database that holds metadata for all installed bundles'''

    Dbci = namedtuple('Dbc', 'dsn_template sql') #Database connection information 
   
    DBCI = {
            'postgres':Dbci(dsn_template='postgresql+psycopg2://{user}:{password}@{server}/{name}',sql='support/configuration-pg.sql'), # Stored in the databundles module. 
            'sqlite':Dbci(dsn_template='sqlite:///{name}',sql='support/configuration.sql')
            }
    
    def __init__(self,  driver=None, server=None, dbname = None, username=None, password=None):
        self.driver = driver
        self.server = server
        self.dbname = dbname
        self.username = username
        self.password = password
   
        self.dsn_template = self.DBCI[self.driver].dsn_template
        self.dsn = None
        self.sql = self.DBCI[self.driver].sql
        
        self._session = None
        self._engine = None
        
        self.logger = databundles.util.get_logger(__name__)
        import logging
        self.logger.setLevel(logging.INFO) 
        
    @property
    def engine(self):
        '''return the SqlAlchemy engine for this database'''
        from sqlalchemy import create_engine  
        
        if not self._engine:
          
            self.dsn = self.dsn_template.format(user=self.username, password=self.password, 
                            server=self.server, name=self.dbname)

            self._engine = create_engine(self.dsn, echo=False) 
            
            from sqlalchemy import event
            event.listen(self._engine, 'connect', _pragma_on_connect)
            
            
        return self._engine

    @property
    def inspector(self):
        from sqlalchemy.engine.reflection import Inspector

        return Inspector.from_engine(self.engine)

    @property
    def session(self):
        '''Return a SqlAlchemy session'''
        from sqlalchemy.orm import sessionmaker
        
        if not self._session:    
            self.Session = sessionmaker(bind=self.engine)
            self._session = self.Session()
            
        return self._session
   
    def close(self):
        raise Exception()
        if self._session:    
            self._session.bind.dispose()
            self.Session.close_all()
            self.engine.dispose() 
            self._session = None
            self._engine = None
            
   
    def commit(self):
        self.session.commit()     
        
    def exists(self):
        
        self.engine
        
        if self.driver == 'sqlite':
            return os.path.exists(self.dbname)
        else :
            return True; # Don't know how to check for a postgres database. 
        
    
    def clean(self):
        s = self.session
        from databundles.orm import Column, Partition, Table, Dataset, Config, File
        
        s.query(Column).delete()
        s.query(Partition).delete()
        s.query(Table).delete()
        s.query(Dataset).delete()
        s.query(Config).delete()
        s.query(File).delete()
        s.commit()
        
        
    def create(self):
        
        """Create the database from the base SQL"""
        if not self.exists():    
         
            try:   
                script_str = os.path.join(os.path.dirname(databundles.__file__),
                                          self.PROTO_SQL_FILE)
            except:
                # Not sure where to find pkg_resources, so this will probably
                # fail. 
                from pkg_resources import resource_string #@UnresolvedImport
                
                script_str = resource_string(databundles.__name__, self.sql)
            
            self.load_sql(script_str)
            
            return True
        
        return False
    
    def drop(self):
        s = self.session
      
        s.execute("DROP TABLE IF EXISTS files")
        s.execute("DROP TABLE IF EXISTS columns")
        s.execute("DROP TABLE IF EXISTS partitions")
        s.execute("DROP TABLE IF EXISTS tables")
        s.execute("DROP TABLE IF EXISTS config")
        s.execute("DROP TABLE IF EXISTS datasets")
        s.commit()


    def load_sql(self, sql_text):
        
        #conn = self.engine.connect()
        #conn.close()
        
        if self.driver == 'postgres':
            import psycopg2
            
            dsn = ("host={} dbname={} user={} password={} "
                    .format(self.server, self.dbname, self.username, self.password))
           
            conn = psycopg2.connect(dsn)
         
            cur = conn.cursor()
          
            cur.execute('DROP TABLE IF EXISTS columns')
            cur.execute('DROP TABLE IF EXISTS partitions')
            cur.execute('DROP TABLE IF EXISTS tables')
            cur.execute('DROP TABLE IF EXISTS config')
            cur.execute('DROP TABLE IF EXISTS datasets')
            cur.execute('DROP TABLE IF EXISTS files')
            
            cur.execute("COMMIT")
            cur.execute(sql_text) 
            cur.execute("COMMIT")
            
            conn.close()
        elif self.driver == 'sqlite':
            
            import sqlite3
            
            dir_ = os.path.dirname(self.dbname)
            if not os.path.exists(dir_):
                try:
                    os.makedirs(dir_) # MUltiple process may try to make, so it could already exist
                except Exception as e: #@UnusedVariable
                    pass
                
                if not os.path.exists(dir_):
                    raise Exception("Couldn't create directory "+dir_)
            
            try:
                conn = sqlite3.connect(self.dbname)
            except:
                self.logger.error("Failed to open Sqlite database: {}".format(self.dbname))
                raise
                
            conn.execute('DROP TABLE IF EXISTS columns')
            conn.execute('DROP TABLE IF EXISTS partitions')
            conn.execute('DROP TABLE IF EXISTS tables')
            conn.execute('DROP TABLE IF EXISTS config')
            conn.execute('DROP TABLE IF EXISTS datasets')
            conn.execute('DROP TABLE IF EXISTS files')
            
            conn.commit()
            conn.executescript(sql_text)  
        
            conn.commit()

        else:
            raise RuntimeError("Unknown database driver: {} ".format(self.driver))
        

    def install_bundle_file(self, identity, bundle_file):
        """Install a bundle in the database, starting from a file that may
        be a partition or a bundle"""

        
        if isinstance(identity , dict):
            identity = new_identity(identity)
            
        if identity.is_bundle:
            bundle = DbBundle(bundle_file)
            
            self.install_bundle(bundle)
        
        
    def install_bundle(self, bundle):
        '''Copy the schema and partitions lists into the library database
        
        '''
        from databundles.orm import Dataset
        from databundles.bundle import Bundle
                
        bdbs = bundle.database.session 
        s = self.session
        dataset = bdbs.query(Dataset).one()
        partition = None
        s.merge(dataset)
        s.commit()

        if not isinstance(bundle, Bundle):
            raise ValueError("Can only install a  Bundle object")

            # The Tables only get installed when the dataset is installed, 
            # not for the partition
 
        for table in dataset.tables:
            try:
                s.merge(table)
                s.commit()
            except IntegrityError as e:
                self.logger.error("Failed to merge table "+str(table.id_)+":"+ str(e))
                s.rollback()
                raise e
         
            for column in table.columns:
                try:
                    s.merge(column)
                    s.commit()
                except IntegrityError as e:
                    self.logger.error("Failed to merge column "+str(column.id_)+":"+ str(e) )
                    s.rollback()
                    raise e

        for partition in dataset.partitions:
            try:
                s.merge(partition)
                s.commit()
            except IntegrityError as e:
                self.logger.error("Failed to merge partition "+str(partition.identity.id_)+":"+ str(e))
                s.rollback()
                raise e

        s.commit()
        
    def remove_bundle(self, bundle):
        '''remove a bundle from the database'''
        
        from databundles.orm import Dataset
        
        s = self.session
        
        try:
            dataset, partition = self.get(bundle.identity) #@UnusedVariable
        except AttributeError:
            dataset, partition = self.get(bundle) #@UnusedVariable
            
            
        if not dataset:
            return False

        dataset = s.query(Dataset).filter(Dataset.id_==dataset.identity.id_).one()

        # Can't use delete() on the query -- bulk delete queries do not 
        # trigger in-ython cascades!
        s.delete(dataset)
  
        s.commit()
        
      
    def get(self,bp_id):
        '''Get a bundle or partition
        
        Gets a bundle or Partition object, referenced by a string generated by
        DatasetNumber or PartitionNumber, or by an object that has a name
        or id_ field. 
        
        Args:
            bp_id (Bundle|Partition|str) Specifies a bundle or partition to
                fetch. bp_id may be:
                    An ObjectNumber string id for a partition or dataset
                    An ObjectNumber object
                    An Identity object, for a partition or bundle
                    Any object that has an 'identity' attribute that is an Identity object
                    
                
        Returns:
            Bundle|Partition
        
        '''

        from databundles.identity import ObjectNumber, PartitionNumber, Identity
        from databundles.orm import Dataset
        from databundles.orm import Partition
        import sqlalchemy.orm.exc 
        
        s = self.session
    
        if isinstance(bp_id, basestring):
            bp_id = ObjectNumber.parse(bp_id)
        elif isinstance(bp_id, ObjectNumber):
            pass
        elif isinstance(bp_id, Identity):
            if not bp_id.id_:
                raise Exception("Identity does not have an id_ defined")
            bp_id = ObjectNumber.parse(bp_id.id_)
            
        else:
            # hope that is has an identity field
            bp_id = ObjectNumber.parse(bp_id.identity.id_)

        dataset = None
        partition = None

        try:
            if isinstance(bp_id, PartitionNumber):
                query = s.query(Dataset, Partition).join(Partition).filter(Partition.id_ == str(bp_id)) 

                dataset, partition = query.one();
                    
            else:
                query = s.query(Dataset)
                query = s.query(Dataset).filter(Dataset.id_ == str(bp_id)) 

                dataset = query.one();
        except sqlalchemy.orm.exc.NoResultFound as e: #@UnusedVariable
            return None, None
            #raise ResultCountError("Failed to find dataset or partition for: {}".format(str(bp_id)))
        
        return dataset, partition
        
    def find(self, query_command):
        '''Find a bundle or partition record by a QueryCommand or Identity
        
        Args:
            query_command. QueryCommand or Identity
            
        returns:
            An SqlAlchemy query that will either return rows with:
            
                ( dataset, dataset_id)
            or
                ( dataset, partition, dataset_id)
                
            The partition for is returned when the QueryCommand includes
            a 'partition' component
            
        '''
      
        from databundles.orm import Dataset
        from databundles.orm import Partition
        from databundles.identity import Identity
        from databundles.orm import Table
        s = self.session
        
        if isinstance(query_command, Identity):
            raise NotImplementedError()
            out = []
            for d in self.queryByIdentity(query_command).all():
                id_ = d.identity
                d.path = os.path.join(self.cache,id_.path+'.db')
                out.append(d)
        
        if len(query_command.partition) == 0:
            query = s.query(Dataset, Dataset.id_) # Dataset.id_ is included to ensure result is always a tuple
        else:
            query = s.query(Dataset, Partition, Dataset.id_)
        
        if len(query_command.identity) > 0:
            for k,v in query_command.identity.items():
                try:
                    query = query.filter( getattr(Dataset, k) == v )
                except AttributeError:
                    # Dataset doesn't have the attribute, so ignore it. 
                    pass
                
        
        if len(query_command.partition) > 0:     
            query = query.join(Partition)
            for k,v in query_command.partition.items():
                
                if k == 'any': continue # Just join the partition
                
                if k == 'table':
                    # The 'table" value could be the table id
                    # or a table name
                    from sqlalchemy.sql import or_
                    
                    query = query.join(Table)
                    query = query.filter( or_(Partition.t_id  == v,
                                              Table.name == v))
                else:
                    query = query.filter(  getattr(Partition, k) == v )
        
        if len(query_command.table) > 0:
            query = query.join(Table)
            for k,v in query_command.table.items():
                query = query.filter(  getattr(Table, k) == v )

        return query

        
    def queryByIdentity(self, identity):
        from databundles.orm import Dataset, Partition
        from databundles.identity import Identity
        from databundles.partition import PartitionIdentity
        from sqlalchemy import desc
        
        s = self.database.session
        
        # If it is a string, it is a name or a dataset id
        if isinstance(identity, str) or isinstance(identity, unicode) : 
            query = (s.query(Dataset)
                     .filter( (Dataset.id_==identity) | (Dataset.name==identity)) )
        elif isinstance(identity, PartitionIdentity):
            
            query = s.query(Dataset, Partition)
            
            for k,v in identity.to_dict().items():
                d = {}
              
                if k == 'revision':
                    v = int(v)
                    
                d[k] = v
         
            query = query.filter_by(**d)
                
        elif isinstance(identity, Identity):
            query = s.query(Dataset)
            
            for k,v in identity.to_dict().items():
                d = {}
                d[k] = v
                
            query = query.filter_by(**d)

           
        elif isinstance(identity, dict):
            query = s.query(Dataset)
            
            for k,v in identity.items():
                d = {}
                d[k] = v
                query = query.filter_by(**d)
      
        else:
            raise ValueError("Invalid type for identity")
    
        query.order_by(desc(Dataset.revision))
     
        return query

        
    def add_file(self,path, group, ref, state='new'):
        from databundles.orm import  File
        stat = os.stat(path)
      
        s = self.session
      
        s.query(File).filter(File.path == path).delete()
      
        file_ = File(path=path, 
                     group=group, 
                     ref=ref,
                     modified=stat.st_mtime, 
                     state = state,
                     size=stat.st_size)
    
        s.add(file_)
        s.commit()

    def get_file_by_state(self, state):
        """Return all files in the database with the given state"""
        from databundles.orm import  File
        s = self.session
        if state == 'all':
            return s.query(File).all()
        else:
            return s.query(File).filter(File.state == state).all()

    def remove_file(self,path):
        pass
    
    def get_file(self,path):
        pass

        
class QueryCommand(object):
    '''An object that contains and transfers a query for a bundle
    
    Components of the query can include. 
    
    Identity
        id
        source
        dataset
        subset
        variation
        creator
        revision

    
    Column 
        name, altname
        description
        keywords
        datatype
        measure 
        units
        universe
    
    Table
        name, altname
        description
        keywords
    
    Partition
        time
        space
        table
        other
        
    When the Partition search is included, the other three components are used
    to find a bundle, then the pretition information is used to select a bundle

    All of the  values are text, except for revision, which is numeric. The text
    values are used in an SQL LIKE phtase, with '%' replaced by '*', so some 
    example values are: 
    
        word    Matches text field, that is, in it entirety, 'word'
        word*   Matches a text field that begins with 'word'
        *word   Matches a text fiels that
    
    '''

    def __init__(self, dict_ = None):
        
        if dict_ is None:
            dict_ = {}
        
        self._dict = dict_
    
    def to_dict(self):
        return self._dict
    
    def from_dict(self, dict_):
        for k,v in dict_.items():
            print "FROM DICT",k,v

    
    def getsubdict(self, group):
        '''Fetch a confiration group and return the contents as an 
        attribute-accessible dict'''

        if not group in self._dict:
            self._dict[group] = {}
            
        inner = self._dict[group]
        query = self

        return _qc_attrdict(inner, query)

    @property
    def identity(self):
        '''Return an array of terms for identity searches''' 
        return self.getsubdict('identity')
    
    @identity.setter
    def identity(self, value):
        self._dict['identity'] = value
    
    @property
    def table(self):
        '''Return an array of terms for table searches'''
        return self.getsubdict('table')
    
    @property
    def column(self):
        '''Return an array of terms for column searches'''
        return self.getsubdict('column')
    
    @property
    def partition(self):
        '''Return an array of terms for partition searches'''
        return self.getsubdict('partition')  
         

    def __str__(self):
        return str(self._dict)



class _qc_attrdict(object):
    

    def __init__(self, inner, query):
        self.__dict__['inner'] = inner
        self.__dict__['query'] = query
        
    def __setattr__(self, key, value):
        #key = key.strip('_')
        inner = self.__dict__['inner']
        inner[key] = value

    def __getattr__(self, key):
        #key = key.strip('_')
        inner = self.__dict__['inner']
        
        if key not in inner:
            return None
        
        return inner[key]
    
    def __len__(self):
        return len(self.inner)
    
    def __iter__(self):
        return iter(self.inner)
    
    def items(self):
        return self.inner.items()
    
    def __call__(self, **kwargs):
        for k,v in kwargs.items():
            self.inner[k] = v
        return self.query
            
class Library(object):
    '''
    
    '''
    import collections

    # Return value for get()
    Return = collections.namedtuple('Return',['bundle','partition'])

    def __init__(self, cache,database, remote=None, sync=False):
        '''
        Libraries are constructed on the root cache name for the library. 
        If the cache does not exist, it will be created. 
        
        Args:
        
            cache: a path name to a directory where bundle files will be stored
            database: 
            remote: URL of a remote library, for fallback for get and put. 
            sync: If true, put to remote synchronously. Defaults to False. 
   
        
        '''
        from  databundles.client.rest import Rest 
        
        self.cache = cache
        self._database = database
        self.remote = remote
        self.sync = sync
        self.api = None
        self.bundle = None # Set externally in bundle.library()

        if not self.cache:
            raise ConfigurationError("Must specify library.cache for the library in bundles.yaml")

        if self.remote:
            self.api =  Rest(self.remote)

        self.logger = logging.getLogger(__name__)

    
    @property
    def database(self):
        '''Return databundles.database.Database object'''
        return self._database
  
        
    def _get_bundle_path_from_id(self, bp_id):
        
        try:
            # Assume it is an Identity, or Identity-like
            dataset, partition = self.database.get(bp_id.id_)
            
            return bp_id.path+".db", dataset, partition
        except AttributeError:
            pass
        
            
        dataset, partition = self.database.get(bp_id)

        if not dataset:
            return None, None, None
            
        if partition:
            rel_path = partition.identity.path+".db"
        else:
            rel_path = dataset.identity.path+".db"
            
        return rel_path, dataset, partition
        
    def get_ref(self,bp_id):
        from databundles.identity import ObjectNumber, DatasetNumber, PartitionNumber, Identity
                
        if isinstance(bp_id, Identity):
            if bp_id.id_:
                bp_id = bp_id.id_
            else:
                bp_id = bp_id.name
                
        # If dataset is not None, it means the file already is in the cache.
        dataset = None
        is_local = True
        try:
            on = ObjectNumber.parse(bp_id)

            if not ( isinstance(on, DatasetNumber) or isinstance(on, PartitionNumber)):
                raise ValueError("Object number must be for a Dataset or Partition: {} ".format(bp_id))
            
            rel_path, dataset, partition  = self._get_bundle_path_from_id(bp_id) #@UnusedVariable
        except: 
            pass
        
        # Try it as a dataset name
        if not dataset:
            r = self.find(QueryCommand().identity(name = bp_id) ).first()
            
            if r:
                rel_path, dataset, partition  = self._get_bundle_path_from_id(r[0].id_) 

        # Try the name as a partition name
        if not dataset:
            q = self.find(QueryCommand().partition(name = bp_id) )
        
            r = q.first()
            if r:
                rel_path, dataset, partition  = self._get_bundle_path_from_id(r[1].id_)         


        # No luck so far, so now try to get it from the remote library
        if not dataset and self.api:
            from databundles.identity import Identity, PartitionIdentity
            import socket
            
            try:
                r = self.api.find(bp_id)
                
                
                if r:
                    r = r[0]
    
                    if hasattr(r, 'Partition') and r.Partition is not None:
                        identity = PartitionIdentity(**(r.Partition._asdict()))
                        dataset = r.Dataset
                        partition = r.Partition
                    else:
                        identity = Identity(**(r.Dataset._asdict()))
                        dataset = r.Dataset
                        partition = None
                        
                    rel_path = identity.path+".db"
        
                    is_local = False
            except socket.error:
                self.logger.error("COnnection to remote {} failed".format(self.remote))
                


        if not dataset:
            return False, False, False, False
        
        return  rel_path, dataset, partition, is_local

           
    def get(self,bp_id):
        '''Get a bundle, given an id string or a name '''

        # Get a reference to the dataset, partition and relative path
        # from the local database. 
        rel_path, dataset, partition, is_local = self.get_ref(bp_id)

        # Try to get the file from the cache. 
        if rel_path:
            abs_path = self.cache.get(rel_path)
        else:
            abs_path = None
     
        # Not in the cache, try to get it from the remote library, 
        # if a remote was set. 
        bundle = None
        if not abs_path and self.api and is_local is False and dataset is not False:
            from databundles.identity import Identity, PartitionIdentity

            identity = ( PartitionIdentity(**(partition._asdict())) if partition 
                         else Identity(**(dataset._asdict())) )
            try:
                r = self.api.get(identity.id_)
                abs_path = self.cache.put(r,rel_path)
                bundle = DbBundle(abs_path)
                self.put_file(identity, abs_path, bundle, state='pulled')
                
                self.database.add_file(abs_path, self.cache.repo_id, bundle.identity.id_, 'pulled')
                     
                self.database.install_bundle(bundle)
                
            except:
                raise

        if not abs_path or not os.path.exists(abs_path):
            return False
       
        if not bundle:
            bundle = DbBundle(abs_path)
            
        bundle.library = self
     
        if partition is not None:
            p =  bundle.partitions.partition(partition)
            
            # Need to set the library so the partition  can determine its path
            # to the database. 
            p.library = self
            
            return self.Return(bundle, partition)
            
        else:
            return self.Return(bundle, None)
        
    def find(self, query_command):
        return self.database.find(query_command)
        
    def dep(self,name):
        """"Bundle version of get(), which uses a key in the 
        bundles configuration group 'dependencies' to resolve to a name"""
        
        deps = self.bundle.config.group('dependencies')
        
        if not deps:
            raise ConfigurationError("Configuration has no 'dependencies' group")
        
        bundle_name = deps.get(name, False)
        
        if not bundle_name:
            raise ConfigurationError("No dependency names '{}'".format(name))
        
        b = self.get(bundle_name)
        
        if not b:
            self.bundle.error("Failed to get key={}, id={}".format(name, bundle_name))
            return False
        
        return b


        
    def put_file(self, identity, file_path, state='new'):
        '''Store a dataset or partition file, without having to open the file
        to determine what it is, by using  seperate identity''' 
        
        if isinstance(identity , dict):
            identity = new_identity(identity)
        
        rel_path = identity.path+".db"
        
        dst = self.cache.put(file_path,rel_path)

        if self.api and self.sync:
            self.api.put(file_path)

        self.database.add_file(dst, self.cache.repo_id, identity.id_,  state)

        if identity.is_bundle:
            self.database.install_bundle_file(identity, file_path)

        return dst, rel_path, self.cache.public_url_f()(rel_path)
     
    def put(self, bundle):
        '''Install a bundle or partition file into the library.
        
        :param bundle: the file object to install
        :rtype: a `Partition`  or `Bundle` object
        
        '''
        from bundle import Bundle
        from partition import Partition
        
        if not isinstance(bundle, (Partition, Bundle)):
            raise ValueError("Can only install a Partition or Bundle object")
        
        # In the past, Partitions could be cloaked as Bundles. Disallow this 
        if isinstance(bundle, Bundle) and bundle.db_config.info.type == 'partition':
            raise RuntimeError("Don't allow partitions cloaked as bundles anymore ")
        
        bundle.identity.name # throw exception if not right type. 

        dst, rel_path, url = self.put_file(bundle.identity, bundle.database.path)

        return dst, rel_path, url

    def remove(self, bundle):
        '''Remove a bundle from the library, and delete the configuration for
        it from the library database'''
        
        self.database.remove_bundle(bundle)
        
        self.cache.remove(bundle.identity.path+".db")
  
    @property
    def datasets(self):
        '''Return an array of all of the dataset records in the library database'''
        from databundles.orm import Dataset
       
        return [d for d in self.database.session.query(Dataset).all()]

  
    def rebuild(self):
        '''Rebuild the database from the bundles that are already installed
        in the repositry cache'''
  
        from databundles.bundle import DbBundle
   
        bundles = []
        for r,d,f in os.walk(self.cache.cache_dir): #@UnusedVariable
            for file_ in f:
                
                if file_.endswith(".db"):
                    try:
                        b = DbBundle(os.path.join(r,file_))
                        # This is a fragile hack -- there should be a flag in the database
                        # that diferentiates a partition from a bundle. 
                        f = os.path.splitext(file_)[0]
    
                        if b.db_config.get_value('info','type') == 'bundle':
                            self.logger.info("Queing: {} from {}".format(b.identity.name, file_))
                            bundles.append(b)
                            
                    except Exception as e:
                        self.logger.error('Failed to process {} : {} '.format(file_, e))

        self.database.clean()
        
        for bundle in bundles:
            self.logger.info('Installing: {} '.format(bundle.identity.name))
            self.database.install_bundle(bundle)
            
    
        
        self.database.commit()
        return bundles
  
    @property
    def new_files(self):
        '''Generator that returns files that should be pushed to the remote
        library'''
        
        new_files = self.database.get_file_by_state('new')
   
        for nf in new_files:
            yield nf
        
  
    def push(self, file_=None):
        """Push any files marked 'new' to the remote
        
        Args:
            file_: If set, push a single file, obtailed from new_files. If not, push all files. 
        
        """
        
        if not self.api:
            raise Exception("Can't push() without defining a remote. ")
 
        if file_ is not None:
            self.api.put(file_.ref, file_.path)
            file_.state = 'pushed'
            self.database.commit()
        else:
            for file_ in self.new_files:
                self.push(file_)
    

def _pragma_on_connect(dbapi_con, con_record):
    '''ISSUE some Sqlite pragmas when the connection is created'''
    
    #dbapi_con.execute('PRAGMA foreign_keys = ON;')
    return # Not clear that there is a performance improvement. 
    dbapi_con.execute('PRAGMA journal_mode = MEMORY')
    dbapi_con.execute('PRAGMA synchronous = OFF')
    dbapi_con.execute('PRAGMA temp_store = MEMORY')
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('pragma foreign_keys=ON')
