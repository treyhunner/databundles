'''
A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it. 
'''

from databundles.run import  RunConfig

import os.path
import shutil
import databundles.database 

from databundles.exceptions import ResultCountError, ConfigurationError

class LibraryDb(object):
    '''Represents the Sqlite database that holds metadata for all installed bundles'''
    def __init__(self, driver=None, server=None, username=None, password=None):
        
      
    @property
    def engine(self):
        '''return the SqlAlchemy engine for this database'''
        from sqlalchemy import create_engine  
        
        if not self._engine:
            self._engine = create_engine('sqlite:///'+self.path, echo=False) 
            
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
            Session = sessionmaker(bind=self.engine)
            self._session = Session()
            
        return self._session
   
    def close(self):
        if self._session:    
            self._session.close()
            self._session = None
   
    def commit(self):
        self.session.commit()     
        
    def exists(self):
        return True
    
    def delete(self):
        pass
         
    def create(self):
        pass
    
    def load_sql(self, sql_file):
        pass
        
    
class BundleQueryCommand(object):
    '''An object that contains and transfers a query for a bundle
    
    Components of the query can include. 
    
    Identity
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
        pass


    @property
    def identity(self):
        '''Return an array of terms for identity searches''' 
        pass
    
    @property
    def table(self):
        '''Return an array of terms for table searches'''
        pass
    
    @property
    def column(self):
        '''Return an array of terms for column searches'''
        pass
    
    @property
    def partition(self):
        '''Return an array of terms for partition searches'''
        pass   

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
    
    def connect_upsream(self, url):
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


    @property
    def root(self):
        return self.directory
        
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
        
    def get(self,bundle_id):
        from databundles.bundle import Bundle as BaseBundle
        datasets = self.findByIdentity(bundle_id)
        
        if len(datasets) == 0:
            raise ResultCountError("Didn't get a result for identity search of: "+
                                   str(bundle_id))
        
        
        return BaseBundle(datasets.pop(0).path)
        
    
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
        return LibraryDb(os.path.join(self.directory,'library.db'))
  
    def install_database(self, bundle):
        '''Copy the schema and partitions lists into the library database'''
        from databundles.orm import Dataset
        from databundles.orm import Partition as OrmPartition
        bdbs = bundle.database.session 
        s = self.database.session
        dataset = bdbs.query(Dataset).one()
        s.merge(dataset)
        s.commit()
        
        for t in dataset.tables:
            s.merge(t)
            
            for c in t.columns:
                s.merge(c)
            
        for p in dataset.partitions:
            from sqlalchemy import or_
            s.query(OrmPartition).filter(
                or_(OrmPartition.id_ == p.id_,OrmPartition.name == p.name)
                ).delete()
            s.merge(p)
            
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

        
    def queryByIdentity(self, identity):
        from databundles.orm import Dataset
        from databundles.identity import Identity
        from sqlalchemy import desc
        
        s = self.database.session
        
        # If it is a string, it is a name or a dataset id
        if isinstance(identity, str) or isinstance(identity, unicode) : 
            query = (s.query(Dataset)
                     .filter( (Dataset.id_==identity) | (Dataset.name==identity)) )
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
            raise ValueError("Invalid type for identit")
    
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
    
    @property
    def datasets(self):
        '''Return an array of all of the dataset identities in the library'''
        from databundles.orm import Dataset
       
        return [d.identity for d in self.database.session.query(Dataset).all()]



class RemoteLibrary(Library):
    '''A remote library has its files stored on a remote server.  This class 
    will download and cache the library databse file, keeping it up to date
    when it changes. '''
    
    
    