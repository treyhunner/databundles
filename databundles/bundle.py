'''
Created on Jun 9, 2012

@author: eric
'''

from databundles.database import Database
from databundles.identity import Identity 
from databundles.filesystem import  BundleFilesystem
from databundles.schema import Schema
from databundles.partition import Partitions
import os.path
from databundles.dbexceptions import  ConfigurationError
from databundles.run import RunConfig
import databundles.util


class Bundle(object):
    '''Represents a bundle, including all configuration 
    and top level operations. '''
 
    logger = None
 
    def __init__(self):
        '''
        '''

        self._schema = None
        self._partitions = None
        self._library = None
        self._identity = None

        self.logger = databundles.util.get_logger(__name__)
        
    @property
    def schema(self):
        if self._schema is None:
            self._schema = Schema(self)
            
        return self._schema
    
    @property
    def partitions(self):     
        if self._partitions is None:
            self._partitions = Partitions(self)  
            
        return self._partitions

    @property
    def identity(self):
        '''Return an identity object. '''
        from databundles.run import AttrDict
        
        if not self._identity:
            
            self._identity =  Identity(**self.config.identity)
            
        return self._identity
            
        
    @property
    def library(self):
        '''Return the library set for the bundle, or 
        local library from get_library() if one was not set. '''
          
        import library
        
        if self._library:
            l = self._libarary
        else:
            l =  library.get_library()
            
        l.logger = self.logger
        l.database.logger = self.logger
        
        return l

    @library.setter
    def library(self, value):
        self._library = value

    
class DbBundle(Bundle):

    def __init__(self, database_file):
        '''Initialize a bundle and all of its sub-components. 
        
        If it does not exist, creates the bundle database and initializes the
        Dataset record and Config records from the bundle.yaml file. Through the
        config object, will trigger a re-load of the bundle.yaml file if it
        has changed. 
        
        Order of operations is:
            Create bundle.db if it does not exist
        '''
        
        super(DbBundle, self).__init__()
       
        self.database = Database(self, database_file)
        self.db_config = self.config = BundleDbConfig(self.database)
        
        self.run_args = None
        
    def table_data(self, query):
        '''Return a petl container for a data table'''
        import petl 
        query = query.strip().lower()
        
        if 'select' not in query:
            query = "select * from {} ".format(query)
 
        
        return petl.fromsqlite3(self.database.path, query)
        
class BuildBundle(Bundle):
    '''A bundle class for building bundle files. Uses the bundle.yaml file for
    identity configuration '''

    def __init__(self, bundle_dir=None):
        '''
        '''
        
        super(BuildBundle, self).__init__()
        

        if bundle_dir is None:
            import inspect
            bundle_dir = os.path.abspath(os.path.dirname(inspect.getfile(self.__class__)))
    
            # bundle_dir = Filesystem.find_root_dir()
          
        if bundle_dir is None or not os.path.isdir(bundle_dir):
            from databundles.dbexceptions import BundleError
            raise BundleError("BuildBundle must be constructed on a cache. "+
                              str(bundle_dir) + " is not a directory")
  
        self.bundle_dir = bundle_dir
    
        self._database  = None
   
        # For build bundles, always use the FileConfig though self.config
        # to get configuration. 
        self.config = BundleFileConfig(self.bundle_dir)
     
        self.filesystem = BundleFilesystem(self, self.bundle_dir)
        

        import base64
        self.logid = base64.urlsafe_b64encode(os.urandom(6)) 
        self.ptick_count = 0;


    def parse_args(self,argv):
        import argparse
        
        parser = argparse.ArgumentParser(prog='python bundle.py',
                                         description='Run the bunble build process')
        
        parser.add_argument('phases', metavar='N', type=str, nargs='*',
                       help='Build phases to run')
        
        parser.add_argument('-r','--reset',  default=False, action="store_true",  
                            help='')
        
        parser.add_argument('-t','--test',  default=False, action="store",  
                            nargs='?', type = int, help='Enable bundle-specific test behaviour')
    
        
        parser.add_argument('-b','--build_opt', action='append', help='Set options for the build phase')
        
        parser.add_argument('-m','--multi', default=False, action="store",  
                            nargs='?', type = int, help='Run the build process on multiple processors')
    
        args = parser.parse_args()
        
        if args.build_opt is None:
            args.build_opt = []
            
        
        if len(args.phases) ==  0 or (len(args.phases) == 1 and args.phases[0] == 'all'):    
            args.phases = ['prepare','build', 'install']
      
            self.run_args = args
            
        if args.test is None: # If not specified, is False. If specified with not value, is None
            args.test = 1
            

        if args.multi is None: # If not specified, is False. If specified with not value, is None
            import multiprocessing
            args.multi = multiprocessing.cpu_count()


       
            
        return args

    @property
    def database(self):
        
        if self._database is None:
            self._database  = Database(self)
            
            def add_type(database):
                self.db_config.set_value('info','type','bundle')
                
            self._database._post_create = add_type 
            
         
        return self._database

    @property
    def db_config(self):
        return BundleDbConfig(self.database)

    @classmethod
    def rm_rf(cls, d):
        
        if not os.path.exists(d):
            return
        
        for path in (os.path.join(d,f) for f in os.listdir(d)):
            if os.path.isdir(path):
                cls.rm_rf(path)
            else:
                os.unlink(path)
        os.rmdir(d)

    def clean(self):
        '''Remove all files generated by the build process'''
        self.rm_rf(self.filesystem.build_path())
        
        # Should check for a shared download file -- specified
        # as part of the library; Don't delete that. 
        #if not self.cache_downloads :
        #    self.rm_rf(self.filesystem.downloads_path())

    
    def log(self, message, **kwargs):
        '''Log the messsage'''
        self.logger.info(message)


    def error(self, message, **kwargs):
        '''Log an error messsage'''
        self.logger.error(message)

    def progress(self,message):
        '''print message to terminal, in place'''
        print 'PRG: ',message

    def ptick(self,message):
        '''Writes a tick to the stdout, without a space or newline'''
        import sys
        sys.stdout.write(message)
        sys.stdout.flush()
        
        self.ptick_count += len(message)
       
        if self.ptick_count > 72:
            sys.stdout.write("\n")
            self.ptick_count = 0


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
   
    
    ### Build the final package

    def pre_build(self):
        return True
        
    def build(self):
        return True
    
    def post_build(self):
        return True
    
        
    ### Submit the package to the library
 
    def pre_install(self):
        return True
    
    def install(self):
        return True
        
    def post_install(self):
        return True
    
    ### Submit the package to the repository
 
    def pre_submit(self):
        return True
    
    def submit(self):
        return True
        
    def post_submit(self):
        return True
    

class BundleConfig(object):
   
    def __init__(self):
        pass


class BundleFileConfig(BundleConfig):
    '''Bundle configuration from a bundle.yaml file '''
    
    BUNDLE_CONFIG_FILE = 'bundle.yaml'

    def __init__(self, root_dir):
        '''Load the bundle.yaml file and create a config object
        
        If the 'id' value is not set in the yaml file, it will be created and the
        file will be re-written
        '''

        super(BundleFileConfig, self).__init__()
        
        self.root_dir = root_dir
        self.local_file = os.path.join(self.root_dir,'bundle.yaml')

        self._run_config = RunConfig(self.local_file)
     
   
        # If there is no id field, create it immediately and
        # write the configuration back out. 
   
        if not self._run_config.identity.get('id',False):
            from databundles.identity import DatasetNumber
            self._run_config.id_ = str(DatasetNumber())
            self.rewrite()
   
        if not os.path.exists(self.local_file):
            raise ConfigurationError("Can't find bundle config file: ")

        
    @property
    def config(self): #@ReservedAssignment
        '''Return a dict/array object tree for the bundle configuration'''
        
        return self._run_config

    @property
    def path(self):
        return os.path.join(self.cache, BundleFileConfig.BUNDLE_CONFIG_FILE)

    def rewrite(self):
        '''Re-writes the file from its own data. Reformats it, and updates
        the modification time'''
        import yaml
        
        temp = self.local_file+".temp"
        old = self.local_file+".old"
        
     
        data = self._run_config.dump()
        
        with open(temp, 'w') as f:
            f.write(data)
    
        if os.path.exists(temp):
            os.rename(self.local_file, old)
            os.rename(temp,self.local_file )
            
    def dump(self):
        '''Re-writes the file from its own data. Reformats it, and updates
        the modification time'''
        import yaml
        
        return yaml.dump(self._run_config, indent=4, default_flow_style=False)
   
   
    def __getattr__(self, group):
        '''Fetch a confiration group and return the contents as an 
        attribute-accessible dict'''
        return self._run_config.group(group)

    def group(self, name):
        '''return a dict for a group of configuration items.'''
        
        return self._run_config.group(name)

from databundles.run import AttrDict
class BundleDbConfigDict(AttrDict):
    
    _parent_key = None
    _bundle = None
    
    def __init__(self, bundle):

        super(BundleDbConfigDict, self).__init__()
    
        '''load all of the values'''
        from databundles.orm import Config as SAConfig
        
        for k,v in self.items():
            del self[k]
        
        s = bundle.database.session
        # Load the dataset
        self['identity'] = {}
        for k,v in bundle.dataset.to_dict().items():
            self['identity'][k] = v
            
        for row in s.query(SAConfig).all():
            if row.group not in self:
                self[row.group] = {}
                
            self[row.group][row.key] = row.value
            

    
class BundleDbConfig(BundleConfig):
    ''' Retrieves configuration from the database, rather than the .yaml file. '''

    database = None
    dataset = None

    def __init__(self, database):
        '''Maintain link between bundle.yam file and Config record in database'''
        
        super(BundleDbConfig, self).__init__()
        
        if not database:
            raise Exception("Didn't get database")
        
        self.database = database
        self.dataset = self.get_dataset()
       
    @property
    def dict(self): #@ReservedAssignment
        '''Return a dict/array object tree for the bundle configuration'''
      
        return {'identity':self.dataset.to_dict()}

    def __getattr__(self, group):
        '''Fetch a confiration group and return the contents as an 
        attribute-accessible dict'''
        
        return self.group(group)


    def group(self, group):
        '''return a dict for a group of configuration items.'''
        
        bd = BundleDbConfigDict(self)
      
        return bd.get(group)
    

    def set_value(self, group, key, value):
        from databundles.orm import Config as SAConfig
        
        if self.group == 'identity':
            raise ValueError("Can't set identity group from this interface. Use the dataset")
        
        s = self.database.session
     
        key = key.strip('_')
  
        s.query(SAConfig).filter(SAConfig.group == group,
                                 SAConfig.key == key,
                                 SAConfig.d_id == self.dataset.id_).delete()
        
        o = SAConfig(group=group,
                     key=key,d_id=self.dataset.id_,value = value)
        s.add(o)
        s.commit()       

    def get_dataset(self):
        '''Initialize the identity, creating a dataset record, 
        from the bundle.yaml file'''
        
        from databundles.orm import Dataset
 
        s = self.database.session

        return  (s.query(Dataset).one())

   


    