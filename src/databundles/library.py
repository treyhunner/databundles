'''
A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it. 
'''

from databundles.runconfig import  RunConfig

import os.path
import shutil
import databundles.database 

class LibraryDb(databundles.database.Database):
    
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
            raise ValueError("Must specify a root directory")
            
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
            
        
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
        dst = os.path.join(self.directory, bundle.identity.name+".db")
        
        bundle.log("Copy {} to {}".format(src,dst))
        shutil.copyfile(src,dst)
        
        self.install_database(bundle)
        
    def remove_bundle(self, bundle):
        '''Remove a bundle from the library, and delete the configuration for
        it from the library database'''
        
        self.database_remove(bundle)
        
        path = os.path.join(self.directory, bundle.identity.name+".db")
        
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
        

    def findByName(self, name):
        pass
    
    
    