'''
Created on Jun 9, 2012

@author: eric
'''

import os.path
import exceptions
from database import Database

class PartitionId(object):

    def __init__(self, bundle, time=None, space=None, table=None):
        self.bundle = bundle
        self.time = time
        self.space = space
        self.table = table

    def __str__(self):
        '''Return the parttion component of the name'''
        import re
        return '.'.join([re.sub('[^\w\.]','_',s).lower() 
                         for s in filter(None, [self.pid.time, self.pid.space, 
                                                self.pid.table])])


class Partition(object):
    '''Represents a bundle partition, part of the bunle data broken out in 
    time, space, or by table. '''
    
  
    def __init__(self, bundle, partition_id):
        self.bundle = bundle
        self.pid= partition_id
      
    @property
    def name(self):
        pass
    
    @property
    def database(self):
        pass
 

class Filesystem(object):
    
    BUILD_DIR = 'build'
    
    def __init__(self, bundle, root_directory = None):
        self.bundle = bundle
        if root_directory:
            self.root_directory = root_directory
        else:
            self.root_directory = Filesystem.find_root_directory()
 
        if not os.path.exists(self.path(Filesystem.BUILD_DIR)):
            os.makedirs(self.path(Filesystem.BUILD_DIR),0755)
 
    @staticmethod
    def find_root_dir(cls,testFile='bundle.yaml'):
        '''Find the parent directory that contains the bundle.yaml file '''
        import sys

        d = sys.path[0]
        
        while os.path.isdir(d) and d != '/':
            test =  os.path.normpath(d+'/'+testFile)
            print "D "+test
            if(os.path.isfile(test)):
                return d
            d = os.path.dirname(d)
             
        return None
    
    @property
    def root_dir(self):
        '''Returns the root directory of the bundle '''
        return self.root_directory

    def path(self, *args):
        '''Resolve a path that is relative to the bundle root into an 
        absoulte path'''
     
        return os.path.normpath(self.root_directory+'/'+os.path.join(*args))    

    def directory(self, rel_path):
        '''Resolve a path that is relative to the bundle root into 
        an absoulte path'''
        abs_path = self.path(rel_path)
        if(not os.path.isdir(abs_path) ):
            os.makedirs(abs_path)
        return abs_path
 
 
class Identity(object):
    
    from databundles.config.properties import DbRowProperty
   
    source = DbRowProperty("source",None)
    dataset = DbRowProperty("dataset",None)
    subset = DbRowProperty("subset",None)
    variation = DbRowProperty("variation",None)
    creator = DbRowProperty("creator",None)
    revision = DbRowProperty("revision",None)
    
    def __init__(self, bundle):
        self.bundle = bundle
 
    @property
    def row(self):
        '''Return the dataset row object for this bundle'''
        from databundles.config.orm import Dataset
        session = self.bundle.database.session
        return session.query(Dataset).first()
        
      
    @property
    def creatorcode(self):
        import hashlib
        # Create the creator code if it was not specified. 
        return hashlib.sha1(self.creator).hexdigest()[0:4]
       
    @property
    def name(self):
        return  '-'.join(self.name_parts())
    
    def name_parts(self):
        """Return the parts of the name as a list, for additional processing. """
        name_parts = [];
     
        try: 
            name_parts.append(self.source)
        except:
            raise exceptions.ConfigurationError('Missing identity.source')
  
        try: 
            name_parts.append(self.dataset)
        except:
            raise exceptions.ConfigurationError('Missing identity.dataset')  
        
        try: 
            name_parts.append(self.subset)
        except:
            pass
        
        try: 
            name_parts.append(self.variation)
        except:
            pass
        
        try: 
            name_parts.append(self.creatorcode)
        except:
            raise exceptions.ConfigurationError('Missing identity.creatorcode')
        
             
        try: 
            name_parts.append('r'+str(self.revision))
        except:
            raise exceptions.ConfigurationError('Missing identity.revision')
        
        import re
        return [re.sub('[^\w\.]','_',s).lower() for s in name_parts]
       
   
    def load_from_config(self):
        pass
   
    def write_to_config(self):
        pass
   
class Bundle(object):
    '''Represents a bundle, including all configuration 
    and top level operations. '''
    
    BUNDLE_CONFIG_FILE = 'bundle.yaml'
    SCHEMA_CONFIG_FILE = 'schema.yaml'
    
   
    def __init__(self, directory=None):
        '''
        Constructor
        '''
     
        self.filesystem = Filesystem(self, directory)
        self.identity = Identity(self)
        self.database = Database(self)
        
        ##
        ## Check that we have a dataset in the database. If not, create one, 
        ## since the Identity depends on it. 
        from databundles.config.orm import Dataset
        session = self.database.session
        ds = session.query(Dataset).first()
            
        if not ds:
            self._init_identity()
     
      
    def _init_identity(self):
        '''Initialize the identity, creating a dataset record, 
        from the bundle.yaml file'''
        from databundles.config.orm import Dataset
        c = self.config
        
        ds = Dataset(**c['identity'])
       
        s = self.database.session
        s.add(ds)
        s.commit()
        
        return 
       
    def partition(self, partition_id):
        
        if not self._partition:
            p = self.config.get('partition',
                                {'time':None, 'state': None, 'table': None})
            self._partition=Partition(self,p.get('time', None),
                                      p.get('space', None),p.get('table', None))
       
        return self._partition

    @property
    def config(self):
        '''Return a dict/array object tree for the bundle configuration'''
     
        import yaml
        bundle_path = self.filesystem.path(Bundle.BUNDLE_CONFIG_FILE)

        try:
            return  yaml.load(file(bundle_path, 'r'))  
        except:
            raise NotImplementedError,''' Bundle.yaml missing. 
            Auto-creation not implemented'''

    @property
    def schema(self):
        '''Return the dict form of the schema'''
        if(self._schema == None):
            import yaml
            schema_path = self._root_dir.path(Bundle.SCHEMA_CONFIG_FILE)
    
            try:
                self._schema = yaml.load(file(schema_path, 'r'))  
            except:
                raise NotImplementedError,''' Schema.yaml missing. Auto-creation
                 not implemented'''
  
        return self.config
  
   
    ###
    ### Process Methods
    ###


    ### Prepare is run before building, part of the devel process.  

    def pre_prepare(self):
        return True

    def prepare(self):
        return True
    
    def post_prepare(self):
        return True
   
    ### Download URLS from a list, hand coded, or from prepare() 
   
    def pre_download(self):
        return True  
    
    def download(self):
        return True  
    
    def post_download(self):
        return True  
    
    ### Transform to the database format
 
    def pre_transform(self):
        return True
    
    def transform(self):
        return True
    
    def post_transform(self):
        return True
    
    ### Build the final package

    def pre_build(self):
        return True
        
    def build(self):
        return True
    
    def post_build(self):
        return True
    
        
    ### Submit the package to the repository
 
    def pre_submit(self):
        return True
    
    def submit(self):
        return True
        
    def post_submit(self):
        return True
    
    
    
    