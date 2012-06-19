'''
Created on Jun 9, 2012

@author: eric
'''

import files
import os.path
import exceptions

class Partition(object):
    '''Represents a bundle partition, part of the bunle data broken out in time, space, or by table. '''
    
    def __init__(self, bundle, time=None, space=None, table=None):
        self.bundle = bundle
        self.time = time
        self.space = space
        self.table = table
    
    def get_name_part(self):
        '''Return the parttion component of the name'''
        import re
        return '.'.join([re.sub('[^\w\.]','_',s).lower() for s in filter(None, [self.time, self.space, self.table])])
      
  
    
class Bundle(object):
    '''Represents a bundle, including all configuration and top level operations. '''
    
    BUNDLE_CONFIG_FILE = 'bundle.yaml'
    SCHEMA_CONFIG_FILE = 'schema.yaml'
   
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
        self._partition = None
  
    @property
    def name(self):
       
        name_parts = [];
        
        partition = self.partition
        
        try: 
            name_parts.append(self.identity['source'])
        except:
            raise exceptions.ConfigurationError('Missing identity.source')
  
        try: 
            name_parts.append(self.identity['dataset'])
        except:
            raise exceptions.ConfigurationError('Missing identity.dataset')  
        
        try: 
            name_parts.append(self.identity['subset'])
        except:
            pass
        
        try: 
            name_parts.append(self.identity['variation'])
        except:
            pass
        
        try: 
            name_parts.append(self.identity['creatorcode'])
        except:
            raise exceptions.ConfigurationError('Missing identity.creatorcode')
        
        if partition:
            part = partition.get_name_part()
            if part:
                name_parts.append(part)
                
        try: 
            name_parts.append('r'+str(self.identity['revision']))
        except:
            raise exceptions.ConfigurationError('Missing identity.revision')
        
        import re
        return '-'.join([re.sub('[^\w\.]','_',s).lower() for s in name_parts])
  
    @property
    def partition(self):
        
        if not self._partition:
            p = self.config.get('partition',{'time':None, 'state': None, 'table': None})
            self._partition=Partition(self,p.get('time', None),p.get('space', None),p.get('table', None))
       
        return self._partition
        
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
  
            if not 'creatorcode' in self._config['identity']:
                import hashlib
                # Create the creator code if it was not specified. 
                self._config['identity']['creatorcode'] = hashlib.sha1(self._config['identity']['creator']).hexdigest()[0:4]
  
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

    def directory(self, rel_path):
        '''Resolve a path that is relative to the bundle root into an absoulte path'''
        abs_path = self.path(rel_path)
        if(not os.path.isdir(abs_path) ):
            os.makedirs(abs_path)
        return abs_path
            
    @property
    def protodb(self):
        '''Return the path to the proto.db Sqlite File, which holds the prototype configuration for a bundle'''
       
        import protodb
        
        return protodb.ProtoDB(self)

    @property
    def productiondb(self):
        '''Return the path to the production database, creating one from a copy of the prototype if it does not exist'''
        
        proto_file = self.protodb.path()
        pdb_file = self.name.".db"
        
        import os.path
        
        if not os.path.exists():
            pass
        
        
        
        from sqlalchemy import create_engine,MetaData   
        engine = create_engine('sqlite:///'+self.proto_file)
        metadata = MetaData(bind=engine)

    def get_bundle_db(self, Partition=None):
        '''Get the output bundle database, possibly a partitioned database.'''

    @property
    def identity(self):
        return self.config['identity']

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
    
    
    
    