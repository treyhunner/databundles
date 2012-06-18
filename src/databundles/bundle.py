'''
Created on Jun 9, 2012

@author: eric
'''

import files

class Bundle(object):
    '''Represents a bundle, including all configuration and top level operations. '''
    
    BUNDLE_CONFIG_FILE = 'bundle.yaml'
    SCHEMA_CONFIG_FILE = 'schema.yaml'
    PROTO_DB_FILE = 'proto.db'
    PROTO_SQL_FILE = 'configuration.sql' # Stored in the databundles module. 
    
    def __init__(self, directory=None):
        '''
        Constructor
        '''
       
        if not directory:
            self._root_dir = files.root_dir()
        else:
            self._root_dir = files.RootDir(directory)

        self._config = None
        self._schema = None
  
    @property
    def config(self):
        '''Return a dict/array object tree for the bundle configuration'''
        if(self._config == None):
            import yaml
            bundle_path = self._root_dir.path(Bundle.BUNDLE_CONFIG_FILE)
    
            try:
                self._config = yaml.load(file(bundle_path, 'r'))  
            except:
                raise NotImplementedError,' Bundle.yaml missing. Auto-creation not implemented'
  
        return self._config

    @property
    def schema(self):
        '''Return the dict form of the schema'''
        if(self._schema == None):
            import yaml
            schema_path = self._root_dir.path(Bundle.SCHEMA_CONFIG_FILE)
    
            try:
                self._schema = yaml.load(file(schema_path, 'r'))  
            except:
                raise NotImplementedError,' Schema.yaml missing. Auto-creation not implemented'
  
        return self.config

    def path(self, rel_path):
        '''Resolve a path that is relative to the bundle root into an absoulte path'''
        return self._root_dir.path(rel_path)

    def protodb(self):
        '''Return the path to the proto.db Sqlite File, which holds the prototype configuration for a bundle'''
        import os.path
        proto_file=self.path(Bundle.PROTO_DB_FILE)
        
        if os.path.exists(proto_file):
            return proto_file
     
        import databundles
        script_str = os.path.normpath(os.path.dirname(databundles.__file__)+'/'+Bundle.PROTO_SQL_FILE)
        
        print "!!!!! "+proto_file
        print "!!!!! "+script_str
        
        import sqlite3
        conn = sqlite3.connect(proto_file)
        conn.executescript(open(script_str).read().strip())
        conn.commit()
        
        return proto_file


    def protodb_dsn(self):
        '''return the SqlAlchemy connection string for the protodb'''
        pass

    @property
    def root_dir(self):
        '''Returns the root directory of the bundle '''
        return self._root_dir

    """ Prepare is run before building, part of the devel process.  """

    def pre_prepare(self):
        return True

    def prepare(self):
        return True
    
    def post_prepare(self):
        return True
   
    """ Download URLS from a list, hand coded, or from prepare() """
   
    def pre_download(self):
        return True  
    
    def download(self):
        return True  
    
    def post_download(self):
        return True  
    
    """ Transform to the database format """
 
    def pre_transform(self):
        return True
    
    def transform(self):
        return True
    
    def post_transform(self):
        return True
    
    """ Build the final package """

    def pre_build(self):
        return True
        
    def build(self):
        return True
    
    def post_build(self):
        return True
    
        
    """ Submit the package to the repository """
 
    def pre_submit(self):
        return True
    
    def submit(self):
        return True
        
    def post_submit(self):
        return True
    
    
    
    