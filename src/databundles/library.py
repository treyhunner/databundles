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

def get_library():
    ''' Returns LocalLIbrary singleton'''
    global library
    if library is None:
        library =  LocalLibrary()
        
    return library

class LibraryDb(object):
    '''Represents the Sqlite database that holds metadata for all installed bundles'''
    
   
    PROTO_SQL_FILE = 'support/configuration-pg.sql' # Stored in the databundles module. 
    
    def __init__(self, driver=None, server=None, dbname = None, username=None, password=None):
        self.driver = driver
        self.server = server
        self.dbname = dbname
        self.username = username
        self.password = password
        
        self._session = None
        self._engine = None
        
        
    @property
    def engine(self):
        '''return the SqlAlchemy engine for this database'''
        from sqlalchemy import create_engine  
        
        if not self._engine:
            self._engine = create_engine(
                     'postgresql+psycopg2://{}:{}@{}/{}'
                     .format(self.username, self.password, self.server, self.dbname),
                     echo=False) 
            
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
    
    def clean(self):
        s = self.session
        from orm import Column, Partition, Table, Dataset, Config, File
        
        s.query(Column).delete()
        s.query(Partition).delete()
        s.query(Table).delete()
        s.query(Dataset).delete()
        s.query(Config).delete()
        s.query(File).delete()
        
         
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
                script_str = resource_string(databundles.__name__, self.PROTO_SQL_FILE)
            
            self.load_sql(script_str)
            
            
            return True
        
        return False
    
    def load_sql(self, sql_file):
        
        #conn = self.engine.connect()
        #conn.close()
        
        import psycopg2
        dsn = ("host={} dbname={} user={} password={} "
                .format(self.server, self.dbname, self.username, self.password))
       
        conn = psycopg2.connect(dsn)
        procedures  = open(sql_file,'r').read() 
        cur = conn.cursor()
      
        cur.execute('DROP TABLE IF EXISTS columns')
        cur.execute('DROP TABLE IF EXISTS partitions')
        cur.execute('DROP TABLE IF EXISTS tables')
        cur.execute('DROP TABLE IF EXISTS config')
        cur.execute('DROP TABLE IF EXISTS datasets')
        cur.execute('DROP TABLE IF EXISTS files')
        
        cur.execute("COMMIT")
        cur.execute(procedures) 
        cur.execute("COMMIT")
        
        conn.close()
        
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

    def __init__(self, directory=None, **kwargs):
        '''
        Libraries are constructed on the root directory name for the library. 
        If the directory does not exist, it will be created. 
        '''
        if directory is not None:
            self.directory = directory
        else:
            # Try to get the library directory name from the 
            self.config = kwargs.get('config',RunConfig())
            self.directory = self.config.group('library').get('root',None)
            
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
            
        if not self.directory:
            raise ConfigurationError("Must specify a root directory for the library in bundles.yaml")

        self._named_bundles = kwargs.get('named_bundles', None)

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
        
    def put(self, bundle, **kwargs):
        '''Install a bundle file, and all of its partitions, into the library.
        Copies in the files that don't exist, and loads data into the library
        database'''
        
        # First, check if the bundle is already installed. If so, remove it. 
        self.remove(bundle)

        src = bundle.database.path
        dst = os.path.join(self.directory, bundle.identity.path+".db")
        
        if not os.path.isdir(os.path.dirname(dst)):
            os.makedirs(os.path.dirname(dst))
     
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
                fetch
                
        Returns:
            Bundle|Partition
        
        '''
        from bundle import DbBundle
        from identity import ObjectNumber, DatasetNumber, PartitionNumber, Identity
        from orm import Dataset
        from orm import Partition
        import sqlalchemy.orm.exc 
        
        s = self.database.session
    
        if isinstance(bp_id, basestring):
            bp_id = ObjectNumber.parse(bp_id)
        elif isinstance(bp_id, ObjectNumber):
            pass
        elif isinstance(bp_id, Identity):
           
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
        except sqlalchemy.orm.exc.NoResultFound as e:
            from exceptions import ResultCountError
            return None
            #raise ResultCountError("Failed to find dataset or partition for: {}".format(str(bp_id)))
        
        path = self.path(dataset.identity.path)+".db"
       
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
            
        return self._database
  
    @property
    def datasets(self):
        '''Return an array of all of the dataset identities in the library'''
        from databundles.orm import Dataset
       
        return [d.identity for d in self.database.session.query(Dataset).all()]

  
    def install_database(self, bundle):
        '''Copy the schema and partitions lists into the library database'''
        from databundles.orm import Dataset
        from databundles.orm import Partition as OrmPartition
        from databundles.orm import Table
        from databundles.orm import Column
        from partition import Partition
        
        bdbs = bundle.database.session 
        s = self.database.session
        dataset = bdbs.query(Dataset).one()
        s.merge(dataset)
        s.commit()

        if isinstance(bundle, Partition):
            for partition in dataset.partitions:
                s.merge(partition)
        else:
            for table in dataset.tables:
                s.query(OrmPartition).filter(OrmPartition.t_id == table.id_).delete()
                s.query(Column).filter(Column.t_id == table.id_).delete()
                s.query(Table).filter(Table.id_ == table.id_).delete()
                
                s.merge(table)
             
                for column in table.columns:
                    s.merge(column)
    
        s.commit()
        
    def remove_database(self, bundle):
        '''remove a bundle from the database'''
        
        from databundles.orm import Dataset
        import sqlalchemy.orm.exc
        
        s = self.database.session
        
        try:
            row = s.query(Dataset).filter(Dataset.id_==bundle.identity.id_).one()
            s.delete(row)
            s.commit()
            return
        except sqlalchemy.orm.exc.NoResultFound: 
            pass
            
        try:
            row = s.query(Dataset).filter(Dataset.name==bundle.identity.name).one()
            row.remove();
            s.commit()
            return
        except sqlalchemy.orm.exc.NoResultFound: 
            pass

     
    def find(self, query_command):
        from databundles.orm import Dataset
        from databundles.orm import Partition
        from databundles.identity import Identity
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
                if k == 'table': k = 't_id'
                if k == 'any': continue # Just join the partition
                query = query.filter(  getattr(Partition, k) == v )
        
        if len(query_command.table) > 0:
            from databundles.orm import Table
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
                
            print d 
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

    
    