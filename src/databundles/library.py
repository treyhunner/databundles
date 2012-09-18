'''
A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it. 
'''

from databundles.run import  RunConfig

import os.path
import shutil
import databundles #@UnresolvedImport

from databundles.dbexceptions import ResultCountError, ConfigurationError
import databundles.client.rest 

library = None


def get_cache(config=None):
    if config is None:
        config = RunConfig()    

    if not config.library:
        raise ConfigurationError("Didn't get library configuration value")

    cache_dir = config.library.cache
    
    if not cache_dir:
        raise ConfigurationError("Didn't get library.cache configuration value")
    
    repo_dir = config.library.repository
    
    if repo_dir:
        repo = FsCache(repo_dir)
        cache_size = config.library.cache_size
        if not cache_size:
            cache_size = 10000 # 10 GB
        cache = FsCache(cache_dir, maxsize = cache_size, upstream = repo)
    else:
        cache =  FsCache(cache_dir)  
    
    return cache
    
def get_database(config=None):
    if config is None:
        config = RunConfig()    

    if not config.library:
        raise ConfigurationError("Didn't get library configuration value")
    
    db_config = config.library.database
    
    if not db_config:
        raise ConfigurationError("Didn't get library.cache configuration value")
    
    database = LibraryDb(**db_config)      
    database.create() # creates if does not exist. 
    
    return database

def get_library(config=None):
    ''' Returns LocalLibrary singleton'''
    global library
    if library is None:
        
        if config is None:
            config = RunConfig()
        
        library =  Library(cache = get_cache(config), 
                           database = get_database(config))
    
    return library

def copy_stream_to_file(stream, file_path):
    '''Copy an open file-list object to a file'''

    with open(file_path,'w') as f:
        chunksize = 8192
        chunk =  stream.read(chunksize) #@UndefinedVariable
        while chunk:
            f.write(chunk)
            chunk =  stream.read(chunksize) #@UndefinedVariable

class LibraryDb(object):
    '''Represents the Sqlite database that holds metadata for all installed bundles'''
    from collections import namedtuple
    Dbci = namedtuple('Dbc', 'dsn sql') #Database connection information 
   
    DBCI = {
            'postgres':Dbci(dsn='postgresql+psycopg2://{user}:{password}@{server}/{name}',sql='support/configuration-pg.sql'), # Stored in the databundles module. 
            'sqlite':Dbci(dsn='sqlite:///{name}',sql='support/configuration.sql')
            }
    
    def __init__(self,  driver=None, server=None, dbname = None, username=None, password=None):
        self.driver = driver
        self.server = server
        self.dbname = dbname
        self.username = username
        self.password = password
   
        self.dsn = self.DBCI[self.driver].dsn
        self.sql = self.DBCI[self.driver].sql
        
        self._session = None
        self._engine = None
        
        
    @property
    def engine(self):
        '''return the SqlAlchemy engine for this database'''
        from sqlalchemy import create_engine  
        
        if not self._engine:
          
            dsn = self.dsn.format(user=self.username, password=self.password, 
                            server=self.server, name=self.dbname)

            self._engine = create_engine(dsn, echo=False) 
            
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
            conn = sqlite3.connect(self.dbname)
           
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
        
    def install_bundle(self, bundle):
        '''Copy the schema and partitions lists into the library database
        
        The 'bundle' may be either a bundle or a partition. If it is a bundle, 
        any previous bundle, all of its partitions, are deleted from the database.
        '''
        from databundles import resolve_id
        from databundles.orm import Dataset
        from databundles.orm import Partition as OrmPartition

        from databundles.partition import Partition
        from databundles.bundle import Bundle
                
        bdbs = bundle.database.session 
        s = self.session
        dataset = bdbs.query(Dataset).one()
        partition = None
        s.merge(dataset)
        s.commit()

        if not isinstance(bundle, (Partition, Bundle)):
            raise ValueError("Can only install a Partition or Bindle object")

        if bundle.db_config.info.type == 'partition':
            # This looks like a bundle, but is actually a partition, so
            # we can't get the actual partition
            partition =  bdbs.query(OrmPartition).first()
        
            s.merge(partition)
            s.commit()
           
        elif isinstance(bundle, Partition):

            try: 
                # Find the partition record from the bundle
                
                q = (bdbs
                      .query(OrmPartition)
                      .filter(OrmPartition.id_ == str(resolve_id(bundle))))
      
                partition =  q.one()
 
                s.merge(partition)
                s.commit()
            except Exception as e:
                print "ERROR: Failed to merge partition "+str(bundle.identity)+":"+ str(e)
        else:
            # The Tables only get installed when the dataset is installed, 
            # not for the partition
 
            for table in dataset.tables:
                try:
                    s.merge(table)
                    s.commit()
                except Exception as e:
                    print "ERROR: Failed to merge table"+str(table.id_)+":"+ str(e)
             
                for column in table.columns:
                    try:
                        s.merge(column)
                        s.commit()
                    except Exception as e:
                        print "ERROR: Failed to merge column"+str(column.id_)+":"+ str(e)
    
        s.commit()
        
        return dataset, partition
           
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
            
            return out
        
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

        
    def add_file(self,path, group, ref):
        from databundles.orm import  File
        stat = os.stat(path)
      
        s = self.session
      
        s.query(File).filter(File.path == path).delete()
      
        file_ = File(path=path, group=group, ref=ref,
                    modified=stat.st_mtime, size=stat.st_size)
    
        s.add(file_)
        s.commit()


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

    def __init__(self, dict_ = {}):
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
        
        class attrdict(object):
            def __setattr__(self, key, value):
                #key = key.strip('_')
                inner[key] = value

            def __getattr__(self, key):
                #key = key.strip('_')
                if key not in inner:
                    return None
                
                return inner[key]
            
            def __len__(self):
                return len(inner)
            
            def __iter__(self):
                return iter(inner)
            
            def items(self):
                return inner.items()
            
            def __call__(self, **kwargs):
                for k,v in kwargs.items():
                    inner[k] = v
                return query

        return attrdict()

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


class Library(object):
    '''
    
    '''

    def __init__(self, cache,database, remote=None):
        '''
        Libraries are constructed on the root cache name for the library. 
        If the cache does not exist, it will be created. 
        
        Args:
        
            cache: a path name to a directory where bundle files will be stored
            config: A RunConfig object. If not given, use the default RunConfig
        
        '''
    
        self.cache = cache
        self._database = database
        self.remote = None
        
        if not self.cache:
            raise ConfigurationError("Must specify library.cache for the library in bundles.yaml")

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
            dataset, partition = self.database.get(bp_id)
    
            if not dataset:
                return None, None, None
                
            if partition:
                rel_path = partition.identity.path+".db"
            else:
                rel_path = dataset.identity.path+".db"
                
            return rel_path, dataset, partition
        
    def get(self,bp_id):
        '''Get a bundle, given an identity or an id string '''
        from databundles.bundle import DbBundle

        # If dataset is not None, it means the file already is in the cache.
        rel_path, dataset, partition  = self._get_bundle_path_from_id(bp_id)

        # Try to get the file from the cache. 
        if rel_path:
            abs_path = self.cache.get(rel_path)
        else:
            abs_path = None
     
        # Not in the cache, try to get it from the remote library, 
        # if a remote was set. 
        if not abs_path and self.remote:
            abs_path = self.remote.get(bp_id)
            
            if abs_path:
                # The remote has put the file into our library, 
                # (If the remote is configured with the same cache as the main library)
                # so we need to install the bundle. 
                d2, p2 = self.database.install_bundle(abs_path)
                
            
        if not abs_path or not os.path.exists(abs_path):
            return False
       
        bundle = DbBundle(abs_path)
     
        if partition is not None:
            p =  bundle.partitions.partition(partition)
            
            # Need to set the library so the partition  can determine its path
            # to the database. 
            p.library = self
            
            return p
            
        else:
            return bundle
        
    def find(self, query_command):
        return self.database.find(query_command)
           
    def put(self, bundle):
        '''Install a bundle file,  into the library.
        Copies in the files that don't exist, and loads data into the library
        database'''
        
        bundle.identity.name # throw exception if not right type. 

        src = bundle.database.path
        rel_path = bundle.identity.path+".db"
     
        dst = self.cache.put(src,rel_path)
        self.database.add_file(dst, self.cache.repo_id, bundle.identity.id_)
          
        dataset, partition = self.database.install_bundle(bundle)
        
        return dataset, partition, dst

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
        for r,d,f in os.walk(self.cache): #@UnusedVariable
            for file_ in f:
                if file.endswith(".db"):
                    b = DbBundle(os.path.join(r,file_))
                    # This is a fragile hack -- there should be a flag in the database
                    # that diferentiates a partition from a bundle. 
                    f = os.path.splitext(file_)[0]

                    if b.identity.name.endswith(f):
                        bundles.append(b)

        self.database.clean()
        
        for bundle in bundles:
            self.database.install_bundle(bundle)
            
            for partition in bundle.partitions.all:
                partition.library = self
                self.database.install_bundle(partition)
            
        self.database.commit()
        return bundles
  

class RemoteLibrary(object):
    '''A library that uses a REST Interface to a remote library. 
    
    '''


    def __init__(self, url, cache):
        '''
        Args:
            url. URL of the REST service
            cache. An FsCache for storing files that are retrieved. 
        '''
        from  databundles.client.rest import Rest
    
        r = Rest(url)
        
    def get(self,rel_path):
        '''
        '''

        
        return self.upstream.get(rel_path)
    
    def put(self, source, rel_path):
        '''Put the bundle to the remote server. 
        Does not return the path to teh local file like other caches. 
        '''
        
        # @attention: We're not using rel_path, since the upstream
        # library will make its own decisions about how to set the name
        # for a bundle. 
        
        r =  self.api.put(source)
     
        return r.object
        
    
    def find(self,query):
        '''Passes the query to the upstream, if it exists'''
        return self.database.find(query)
    
    def remove(self,rel_path, propagate = False):
        ''''''
        return self.upstream.remove(rel_path, propagate)
            
        
    def list(self, path=None):
        '''get a list of all of the files in the repository'''
        return self.upstream.list(path)
  


class FsCache(object):
    '''A repository that transfers files to and from a remote filesystem
    
    A Repository is the place that files are stored, usually Amazon S3, or
    a file system. A library can copy files from the repository to its local
    filesystem for use. This can be valuable, even when the repository is a
    filesystem, because Sqlite files don't work well over NFS. '''


    def __init__(self, cache_dir, maxsize=10000, upstream=None):
        '''Init a new FileSystem Cache
        
        Args:
            cache_dir
            maxsize. Maximum size of the cache, in GB
        
        '''

        self.cache_dir = cache_dir
        self.maxsize = int(maxsize * 1048578)  # size in MB
        self.upstream = upstream
        self._database = None
   
        if not os.path.isdir(self.cache_dir):
            os.makedirs(self.cache_dir)
        
        if not os.path.isdir(self.cache_dir):
            raise ConfigurationError("Cache dir '{}' is not valid".format(self.cache_dir)) 
        
    @property
    def database(self):
        import sqlite3
        
        if not self._database:
            db_path = os.path.join(self.cache_dir, 'file_database.db')
            
            if not os.path.exists(db_path):
                create_sql = """
                CREATE TABLE files(
                path TEXT UNIQUE ON CONFLICT REPLACE, 
                size INTEGER, 
                time REAL)
                """
                conn = sqlite3.connect(db_path)
                conn.execute(create_sql)
                conn.close()
                
            self._database = sqlite3.connect(db_path)
            
        return self._database
            
    @property
    def size(self):
        '''Return the size of all of the files referenced in the database'''
        c = self.database.cursor()
        r = c.execute("SELECT sum(size) FROM files")
     
        try:
            size = int(r.fetchone()[0])
        except TypeError:
            size = 0
    
        return size
        
    def delete_to_size(self, size):
        '''Delete records, from oldest to newest, to free up space ''' 
      
        if size <= 0:
            return
      
        for row in self.database.execute("SELECT path, size, time FROM files ORDER BY time ASC"):
            if size < 0: 
                break
        
            #print "DELETE ", row[0], row[1], row[2],size
            self.remove(row[0])
            
            size -= row[1]
  
  
    def free_up_space(self, size):
        '''If there are not size bytes of space left, delete files
        until there is ''' 
        
        space = self.size + size - self.maxsize # Amount of space we are over ( bytes ) for next put
        
        self.delete_to_size(space)

    def add_record(self, rel_path, size):
        import time
        c = self.database.cursor()
        c.execute("insert into files(path, size, time) values (?, ?, ?)", 
                    (rel_path, size, time.time()))
        self.database.commit()

    def verify(self):
        '''Check that the database accurately describes the state of the repository'''
        
        c = self.database.cursor()
        non_exist = set()
        
        no_db_entry = set(os.listdir(self.cache_dir))
        try:
            no_db_entry.remove('file_database.db')
            no_db_entry.remove('file_database.db-journal')
        except: 
            pass
        
        for row in c.execute("SELECT path FROM files"):
            path = row[0]
            
            repo_path = os.path.join(self.cache_dir, path)
        
            if os.path.exists(repo_path):
                no_db_entry.remove(path)
            else:
                non_exist.add(path)
            
        if len(non_exist) > 0:
            raise Exception("Found {} records in db for files that don't exist: {}"
                            .format(len(non_exist), ','.join(non_exist)))
            
        if len(no_db_entry) > 0:
            raise Exception("Found {} files that don't have db entries: {}"
                            .format(len(no_db_entry), ','.join(no_db_entry)))
        
    @property
    def repo_id(self):
        '''Return the ID for this repository'''
        import hashlib
        m = hashlib.md5()
        m.update(self.cache_dir)

        return m.hexdigest()
    
    def get_stream(self, rel_path):
        p = self.get(rel_path)
        
        if not p:
            return None
        
        return open(p)
        
    
    def get(self, rel_path):
        '''Return the file path referenced but rel_path, or None if
        it can't be found. If an upstream is declared, it will try to get the file
        from the upstream before declaring failure. 
        '''
        
        path = os.path.join(self.cache_dir, rel_path)
      
        # The target file always has to exist in the repo
        if  os.path.exists(path):
            
            if not os.path.isfile(path):
                raise ValueError("Path does not point to a file")
            
            return path
            
        if not self.upstream:
            # If we don't have an upstream, then we are done. 
            return None
        
        stream = self.upstream.get_stream(rel_path)
        
        if not stream:
            return None
        
        with open(path,'w') as f:
            shutil.copyfileobj(stream, f)
        
        # Since we've added a file, must keep track of the sizes. 
        size = os.path.getsize(path)
        self.free_up_space(size)
        self.add_record(rel_path, size)
        
        stream.close()
        
        if not os.path.exists(path):
            raise Exception("Failed to copy upstream data to {} ".format(path))
        
        return path
    
    def put(self, source, rel_path):
        '''Copy a file to the repository
        
        Args:
            source: Absolute path to the source file, or a file-like object
            rel_path: path relative to the root of the repository
        
        '''
    
        repo_path = os.path.join(self.cache_dir, rel_path)
      
        if not os.path.isdir(os.path.dirname(repo_path)):
            os.makedirs(os.path.dirname(repo_path))
    
        try:
            shutil.copyfileobj(source, repo_path)
            source.seek(0)
        except AttributeError: 
            shutil.copyfile(source, repo_path)
            
        size = os.path.getsize(repo_path)
        
        self.add_record(rel_path, size)
        
        if self.upstream:
            self.upstream.put(source, rel_path)
    
            # Only delete if there is an upstream
            self.free_up_space(size)

    
        return repo_path
    
    def find(self,query):
        '''Passes the query to the upstream, if it exists'''
        if self.upstream:
            return self.upstream.find(query)
        else:
            return False
    
    def remove(self,rel_path, propagate = False):
        '''Delete the file from the cache, and from the upstream'''
        repo_path = os.path.join(self.cache_dir, rel_path)
        
        if os.path.exists(repo_path):
            os.remove(repo_path)
            
        c = self.database.cursor()
        c.execute("DELETE FROM  files WHERE path = ?", (rel_path,) )
        self.database.commit()
            
        if self.upstream and propagate :
            self.upstream.remove(rel_path)    
            
        
    def list(self, path=None):
        '''get a list of all of the files in the repository'''
        
        path = path.strip('/')
        
        raise NotImplementedError() 

class LibraryDbCache(object):
    '''A cache that implements on the find() method. All others pass through to 
    the mandatory upstream. '''


    def __init__(self, library_db, upstream):
        '''Init a new FileSystem Cache'''

        self.database = library_db
        self.upstream = upstream
   
        
    @property
    def repo_id(self):
        '''Return the ID for this repository'''
        import hashlib
        m = hashlib.md5()
        m.update(self.library_db.path)

        return m.hexdigest()
      
           
    def get(self,rel_path):
        '''Get a dataset or partition by id or name
        '''
     
        return self.upstream.get(rel_path)
    
    def put(self, source, rel_path):
        ''''''

        dst =  self.upstream.put(source, rel_path)
   
        
        return dst

    def find(self,query):
        '''Find records using a query'''
        return self.database.find(query)
    
    def remove(self,rel_path, propagate = False):
        ''''''  
        
        return self.upstream.remove(rel_path, propagate)
             
    def list(self, path=None):
        '''get a list of all of the files in the repository'''
        return self.upstream.list(path)



def _pragma_on_connect(dbapi_con, con_record):
    '''ISSUE some Sqlite pragmas when the connection is created'''
    
    #dbapi_con.execute('PRAGMA foreign_keys = ON;')
    return # Not clear that there is a performance improvement. 
    dbapi_con.execute('PRAGMA journal_mode = MEMORY')
    dbapi_con.execute('PRAGMA synchronous = OFF')
    dbapi_con.execute('PRAGMA temp_store = MEMORY')
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('pragma foreign_keys=ON')
