'''
A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it. 
'''

from databundles.run import  RunConfig

import os.path
import shutil
import databundles

from databundles.exceptions import ResultCountError, ConfigurationError


library = None


def get_library(config=None):
    ''' Returns LocalLIbrary singleton'''
    global library
    if library is None:
        library =  LocalLibrary(config=config)
        
    return library

class LibraryDb(object):
    '''Represents the Sqlite database that holds metadata for all installed bundles'''
    from collections import namedtuple
    Dbci = namedtuple('Dbc', 'dsn sql') #Database connection information 
   
    DBCI = {
            'postgres':Dbci(dsn='postgresql+psycopg2://{user}:{password}@{server}/{name}',sql='support/configuration-pg.sql'), # Stored in the databundles module. 
            'sqlite':Dbci(dsn='sqlite:///{name}',sql='support/configuration.sql')
            }
    
    def __init__(self, driver=None, server=None, dbname = None, username=None, password=None):
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
            return os.path.exists( self.dbname)
        else :
            return True; # Don't know how to check for a postgres database. 
        
    
    def clean(self):
        s = self.session
        from orm import Column, Partition, Table, Dataset, Config, File
        
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
        from orm import Column, Partition, Table, Dataset, Config, File
        
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
            conn = sqlite3.connect( self.dbname)
          
           
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
        
  
        
class BundleQueryCommand(object):
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

    def __init__(self):
        self._dict = {}
        self._library = None

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

    @property 
    def all(self):
        return self._library.find(self).all()
    
    @property 
    def one(self):
        return self._library.find(self).one()    

    @property 
    def first(self):
        
        # Uses limit(), instead of first(), because first()
        # will only return one olbject, while all() will return
        # a tuple of (Dataset, Partition) if that was requested. 
        return self._library.find(self).limit(1).all()[0]

    @property 
    def query(self):
        return self._library.find(self)
    
    def __str__(self):
        return str(self._dict)

class Library(object):
    
    def __init__(self, **kwargs):
        raise NotImplementedError()
    
    def get(self):
        '''Get a bundle by id or name'''
        raise NotImplementedError()
    
    def put(self):
        '''Store a bundle in the library'''
        raise NotImplementedError()
    
    def search(self):
        raise NotImplementedError()
    
    def connect_upstream(self, url):
        '''Connect this library to an upstream library'''
        raise NotImplementedError()
    
    def push(self):
        raise NotImplementedError()
    
    def pull(self, url):
        '''Synchronize the database for a remote library to this library.'''
        raise NotImplementedError()
    
    def datasets(self):
        '''Return an array of all of the dataset identities in the library'''
        raise NotImplementedError()
    
    def dataset(self, id_):
        '''Return an array of all of the dataset identities in the library'''
        raise NotImplementedError()
    
    def query(self):
        q = BundleQueryCommand()
        q._library = self
        return q
    
    
class LocalLibrary(Library):
    '''
    classdocs
    '''

    def __init__(self, directory=None,config=None):
        '''
        Libraries are constructed on the root directory name for the library. 
        If the directory does not exist, it will be created. 
        '''
        if directory is not None:
            self.directory = directory
        else:
            # Try to get the library directory name from the 
            
            self.config = config if config is not None else RunConfig()
       
            self.directory = self.config.group('library').get('root',None)
            
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
            
        if not self.directory:
            raise ConfigurationError("Must specify a root directory for the library in bundles.yaml")

        self._database = None

        

    @property
    def root(self):
        return self.directory
        
    def path(self, *args):
        '''Resolve a path that is relative to the bundle root into an 
        absoulte path'''
     
        args = (self.directory,) + args

        p = os.path.normpath(os.path.join(*args))    
        dir_ = os.path.dirname(p)
        if not os.path.exists(dir_):
            os.makedirs(dir_)

        return p
        
    def put(self, bundle,  remove=True, copy=True):
        '''Install a bundle file, and all of its partitions, into the library.
        Copies in the files that don't exist, and loads data into the library
        database'''
        
        # First, check if the bundle is already installed. If so, remove it. 
        if remove:
            self.remove(bundle)

        src = bundle.database.path
        dst = os.path.join(self.directory, bundle.identity.path+".db")
        
        if not os.path.isdir(os.path.dirname(dst)):
            os.makedirs(os.path.dirname(dst))
     
        if copy:
            shutil.copyfile(src,dst)
        
        self.install_database(bundle)
        
        return dst
        
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
        from bundle import DbBundle
        from identity import ObjectNumber, PartitionNumber, Identity
        from orm import Dataset
        from orm import Partition
        import sqlalchemy.orm.exc 
        
        s = self.database.session
    
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
            return None
            #raise ResultCountError("Failed to find dataset or partition for: {}".format(str(bp_id)))
        
        path = self.path(dataset.identity.path)+".db"
       
        if not os.path.exists(path):
            return False
       
        bundle = DbBundle(path)
        
        if partition is not None:
            p =  bundle.partitions.partition(partition)
            
            # Need to set the library so the partition  can determine its path
            # to the database. 
            p.library = self
            
            return p
            
        else:
            return bundle
    
    def require(self,key):
        from databundles.bundle import DbBundle
        '''Like 'require' but returns a Bundle object. '''
        set_ =  self.findByKey(key)
        
        if len(set_) > 1:
            raise ResultCountError('Got to many results for library require query for key: '+key)
        
        if len(set_) == 0:
            raise ResultCountError('Got no results for library require query for key: '+key)       
        
        return DbBundle(set_.pop(0).path)

    
        
    def remove(self, bundle):
        '''Remove a bundle from the library, and delete the configuration for
        it from the library database'''
        
        self.remove_database(bundle)
        
        path = os.path.join(self.directory, bundle.identity.path+".db")
        
        if os.path.exists(path):
            os.remove(path)
              
    @property
    def database(self):
        '''Return databundles.database.Database object'''
        if self._database is None:
            config = self.config.library.database
     
          
            self._database = LibraryDb(**config)
            
            self._database.create() # creates if does not exist. 
            
        return self._database
  
    @property
    def dataset_ids(self):
        '''Return an array of all of the dataset identities in the library'''
        from databundles.orm import Dataset
       
        return [d.identity for d in self.database.session.query(Dataset).all()]

    @property
    def datasets(self):
        '''Return an array of all of the dataset records in the library database'''
        from databundles.orm import Dataset
       
        return [d for d in self.database.session.query(Dataset).all()]

  
    def install_database(self, bundle):
        '''Copy the schema and partitions lists into the library database'''
        from databundles import resolve_id
        from databundles.orm import Dataset
        from databundles.orm import Partition as OrmPartition
        from databundles.orm import Table
        from databundles.orm import Column
        from partition import Partition
        from bundle import Bundle
                
        bdbs = bundle.database.session 
        s = self.database.session
        dataset = bdbs.query(Dataset).one()
        s.merge(dataset)
        s.commit()

        if not isinstance(bundle, (Partition, Bundle)):
            raise ValueError("Can only install a Partition or Bindle object")

        if isinstance(bundle, Partition):

            try: 
                
                partition =  (bdbs
                              .query(OrmPartition)
                              .filter(OrmPartition.id_ == str(resolve_id(bundle)))
                              .one())
                
                s.merge(partition)
                s.commit()
            except Exception as e:
                        print "ERROR: Failed to merge partition "+str(bundle.identity)+":"+ str(e)
        else:
            pass
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
        
    def rebuild(self):
        '''Rebuild the database from the bundles that are already installed
        in the repositry directory'''
        
        import os.path
        
        from bundle import DbBundle
   
        bundles = []
        for r,d,f in os.walk(self.directory):
            for file in f:
                if file.endswith(".db"):
                    b = DbBundle(os.path.join(r,file))
                    # This is a fragile hack -- there should be a flag in the database
                    # that diferentiates a partition from a bundle. 
                    f = os.path.splitext(file)[0]

                    if b.identity.name.endswith(f):
                        bundles.append(b)

        self.database.clean()
        
        for bundle in bundles:
            self.install_database(bundle)
            
            for partition in bundle.partitions.all:
                partition.library = self
                self.install_database(partition)
            
        self.database.commit()
        return bundles
        
    def remove_database(self, bundle):
        '''remove a bundle from the database'''
        
        from databundles.orm import Dataset, Partition, Table, Column
        import sqlalchemy.orm.exc
        from sqlalchemy.sql import or_
        
        s = self.database.session
        
        b = self.get(bundle.identity)
        
        if not b:
            return False

        
        dataset = s.query(Dataset).filter(Dataset.id_==b.identity.id_).one()

        # Can't use delete() on the query -- bulk delete queries do not 
        # trigger in-ython cascades!
        s.delete(dataset)
  
        s.commit()
        
     
    def find(self, query_command):
        from databundles.orm import Dataset
        from databundles.orm import Partition
        from databundles.identity import Identity
        from databundles.orm import Table
        s = self.database.session
        
        if isinstance(query_command, Identity):
            return self.findByIdentity(query_command)
        
        if len(query_command.partition) == 0:
            query = s.query(Dataset)
        else:
            query = s.query(Dataset, Partition)
        
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
    
    def findByIdentity(self,identity):
        
        out = []
        for d in self.queryByIdentity(identity).all():
            id_ = d.identity
            d.path = os.path.join(self.directory,id_.path+'.db')
            out.append(d)
            
        return out
        
    def findByKey(self,key):
        '''Find a bundle in the library by the shorthand name given in the
        'requires' section of the configuration '''    
        
        if  self._named_bundles is None:
            raise ValueError("Didn't get 'named_bundles' configuration from the constructor")
         
        if key not in self._named_bundles:
            raise ValueError("named_bundles key {} not specified in configuration".format(key))

        return self.findByIdentity(self._named_bundles[key])

    def bundle_db(self,name):
        '''Return a bundle database from the library'''
    

    def stream(self, id_, query=None):
        '''Stream the data from a table in a dataset or a partition 
        
        Args:
            id_: String generated from DatasetNumber, PartitionNumber or TableNumber
            query: A string, or a Table object. 
                If it is a string, it is a query that is passed to the Sqlite database
                If it is a table, the query is "select * from {} ".format(table.name)
        
        returns a PETL table, so the first row will be the headers. 
        
        '''
      
        import petl
        from identity import ObjectNumber, DatasetNumber, PartitionNumber, TableNumber
        
        
        if not isinstance(query,basestring ):
            query = "select * from {} ".format(query.name)
        
        on = ObjectNumber.parse(id_)
        
        if isinstance(on, DatasetNumber):
            dataset = self.get(on)
            database = dataset.database
        elif isinstance(on, PartitionNumber):
            partition = self.get(on)
            database = partition.database
        elif isinstance(on, TableNumber):
            # For table numbers, we will extract the table data from the dataset, 
            # since the table number doesn't specify a partition. 
            # For datasets with partitions, should probably stream all of the
            # tables. 
            pass
        else:
            raise Exception("NUmber {} is not for a dataset nor a partition ".format(id_))
            
        return petl.fromsqlite3(database.path, query)

class RemoteLibrary(Library):
    '''A remote library has its files stored on a remote server.  This class 
    will download and cache the library databse file, keeping it up to date
    when it changes. '''

def _pragma_on_connect(dbapi_con, con_record):
    '''ISSUE some Sqlite pragmas when the connection is created'''
    dbapi_con.execute('PRAGMA foreign_keys = ON;')
    return # Not clear that there is a performance improvement. 
    dbapi_con.execute('PRAGMA journal_mode = MEMORY')
    dbapi_con.execute('PRAGMA synchronous = OFF')
    dbapi_con.execute('PRAGMA temp_store = MEMORY')
    dbapi_con.execute('PRAGMA cache_size = 500000')
    dbapi_con.execute('pragma foreign_keys=ON')

    
    