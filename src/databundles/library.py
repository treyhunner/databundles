'''
A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it. 
'''

from databundles.runconfig import  RunConfig

import os.path
import shutil
import databundles.database 

from databundles.exceptions import ResultCountError, ConfigurationError

class LibraryDb(databundles.database.Database):
    '''Represents the Sqlite database that holds metadata for all installed bundles'''
    def __init__(self, path):
      
        super(LibraryDb, self).__init__(None, path)  

class Library(object):
    '''
    classdocs
    '''

    def __init__(self, directory=None, **kwargs):
        '''
        Libraries are constructed on te root directory name for the library. 
        If the directory does not exist, it will be created. 
        '''

        self.config = kwargs.get('config',RunConfig())

        if directory is not None:
            self.directory = directory
        else:
            self.directory = self.config.group('library').get('root',None)
            
        if not self.directory:
            raise ConfigurationError("Must specify a root directory for the library in bundles.yaml")
            
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
     
        self._requires = kwargs.get('requires',None)
        
    @property
    def root(self):
        return self.directory
        
    def install_bundle(self, bundle):
        '''Install a bundle file, and all of its partitions, into the library.
        Copies in the files that don't exist, and loads data into the library
        database'''
        
        # First, check if the bundle is already installed. If so, remove it. 
        self.remove_bundle(bundle)

        src = bundle.database.path
        dst = os.path.join(self.directory, bundle.identity.path+".db")
        
        if not os.path.isdir(os.path.dirname(dst)):
            os.makedirs(os.path.dirname(dst))
        
        bundle.log("Copy {} to {}".format(src,dst))
        shutil.copyfile(src,dst)
        
        self.install_database(bundle)
        
    def remove_bundle(self, bundle):
        '''Remove a bundle from the library, and delete the configuration for
        it from the library database'''
        
        self.database_remove(bundle)
        
        path = os.path.join(self.directory, bundle.identity.path+".db")
        
        if os.path.exists(path):
            os.remove(path)
              
    @property
    def database(self):
        '''Return databundles.database.Database object'''
        return LibraryDb(os.path.join(self.directory,'library.db'))
        
    def database_remove(self, bundle):
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

    def install_database(self, bundle):
        '''Copy the schema and partitions lists into the library database'''
        from databundles.orm import Dataset
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
            s.merge(p)
            
        s.commit()
        
    def queryByIdentity(self, identity):
        from databundles.orm import Dataset
        from databundles.identity import Identity
        from sqlalchemy import desc
        
        s = self.database.session
        
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
        
        if not self._requires:
            raise ValueError("Didn't get 'requires' configuration from the constructor")
         
        if key not in self._requires:
            raise ValueError("Require key {} not specified in configuration".format(key))

        return self.findByIdentity(self._requires[key])

    def require(self,key):
        from databundles.bundle import Bundle as BaseBundle
        '''Like 'require' but returns a Bundle object. '''
        set_ = self.findByKey(key)
        
        if len(set_) > 1:
            raise ResultCountError('Got to many results for query')
        
        if len(set_) == 0:
            raise ResultCountError('Got no results')       
        
        return BaseBundle(set_.pop(0).path)

    def bundle_db(self,name):
        '''Return a bundle database from the library'''
    