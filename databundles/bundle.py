"""The Bundle object is the root object for a bundle, which includes acessors 
for partitions, schema, and the filesystem

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from databundles.database import Database
from databundles.identity import Identity 
from databundles.filesystem import  BundleFilesystem
from databundles.schema import Schema
from databundles.partition import Partitions
import os.path
from databundles.dbexceptions import  ConfigurationError, ProcessError
from databundles.run import get_runconfig
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
        self._repository = None

        self.logger = databundles.util.get_logger(__name__)
        
        import logging
        self.logger.setLevel(logging.INFO) 
        
        
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
    def repository(self):
        '''Return a repository object '''
        from databundles.repository import Repository
        from databundles.dbexceptions import ConfigurationError       
        
        if not self._repository:
            repo_name = 'default'
            self._repository =  Repository(self, repo_name)
            
        return self._repository
    
    @property
    def identity(self):
        '''Return an identity object. '''
  
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
        l.bundle = self
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
 
        
        return petl.fromsqlite3(self.database.path, query) #@UndefinedVariable
        
class BuildBundle(Bundle):
    '''A bundle class for building bundle files. Uses the bundle.yaml file for
    identity configuration '''

    META_COMPLETE_MARKER = '.meta_complete'

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

        self.run_args = self.args_parser.parse_args(argv)
            
        return self.run_args
    
    @property
    def args_parser(self):
    
        import argparse
        import multiprocessing
        
        parser = argparse.ArgumentParser(prog='dbundle',
                                         description='Manage a DataBundle')
        
        # Commands: meta, prepare, build, install, extract, submit, 
        
        #parser.add_argument('command', nargs=1, help='Create a new bundle') 
     
        parser.add_argument('-c','--config', default=None, action='append', help="Path to a run config file") 
        parser.add_argument('-v','--verbose', default=None, action='append', help="Be verbose") 
        parser.add_argument('-r','--reset',  default=False, action="store_true",  help='')
        parser.add_argument('-t','--test',  default=False, action="store_true", help='Enable bundle-specific test behaviour')
        parser.add_argument('--single-config', default=False,action="store_true", help="Load only the config file specified")
    
        # These are args that Aptana / PyDev adds to runs. 
        parser.add_argument('--port', default=None, help="PyDev Debugger arg") 
        parser.add_argument('--verbosity', default=None, help="PyDev Debugger arg") 
    
        cmd = parser.add_subparsers(title='commands', help='command help')
        
        #
        # Example Command
        #
#        command_p = cmd.add_parser('example', help='Example for the code for a complete command')
#        command_p.set_defaults(command='example')   
#        
#        command_p.add_argument('-a','--aaa', help='First example argument') 
#        command_p.add_argument('-b','--bbb', help='Second example argument') 
#       
#        asp = command_p.add_subparsers(title='example sub-commands', help='Commands for exampling examples')
#    
#        sp = asp.add_parser('subcommand', help='Example subcommand')
#        sp.set_defaults(subcommand='subcommand')
#     
#        sp.add_argument('-a','--aaa', help='First sub-example argument') 
#        sp.add_argument('-b','--bbb', help='Second sub-example argument') 
       
 
        command_p = cmd.add_parser('config', help='Operations on the bundle configuration file')
        command_p.set_defaults(command='config')
           
        asp = command_p.add_subparsers(title='Config subcommands', help='Subcommand for operations on a bundl file')
    
        sp = asp.add_parser('rewrite', help='Re-write the bundle file, updating the formatting')     
        sp.set_defaults(subcommand='rewrite')
        #
        # Clean Command
        #
        command_p = cmd.add_parser('clean', help='Return bundle to state before build, prepare and extracts')
        command_p.set_defaults(command='clean')   
       
        #
        # Meta Command
        #
        command_p = cmd.add_parser('meta', help='Build or install metadata')
        command_p.set_defaults(command='meta')   
        
        command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')     
        
        #action='append_const', const='meta', dest='command', 
                                   
        
        #
        # Prepare Command
        #
        command_p = cmd.add_parser('prepare', help='Prepare by creating the database and schemas')
        command_p.set_defaults(command='prepare')   
        
        command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
        
        #
        # Build Command
        #
        command_p = cmd.add_parser('build', help='Build the data bundle and partitions')
        command_p.set_defaults(command='build')   
        command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
        
        command_p.add_argument('-o','--opt', action='append', help='Set options for the build phase')

        
        command_p.add_argument('-m','--multi',  type = int,  nargs = '?',
                            default = None,
                            const = multiprocessing.cpu_count(),
                            help='Run the build process on multiple processors, if the build method supports it')
        
        #
        # Extract Command
        #
        command_p = cmd.add_parser('extract', help='Extract data into CSV and TIFF files. ')
        command_p.set_defaults(command='extract')   
        command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
        command_p.add_argument('-n','--name', default=None,action="store", help='Run only the named extract, and its dependencies')
        command_p.add_argument('-f','--force', default=False,action="store_true", help='Ignore done_if clauses; force all extracts')


        #
        # Submit Command
        #
        command_p = cmd.add_parser('submit', help='Submit extracts to the repository ')
        command_p.set_defaults(command='submit')    
        command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')   
        command_p.add_argument('-r','--repo',  default=None, help='Name of the repository, defined in the config file')
        command_p.add_argument('-n','--name', default=None,action="store", help='Run only the named extract, and its dependencies')
        command_p.add_argument('-f','--force', default=False,action="store_true", help='Ignore done_if clauses; force all extracts')
    
        #
        # Install Command
        #
        command_p = cmd.add_parser('install', help='Install bundles and partitions to the library')
        command_p.set_defaults(command='install')  
        command_p.add_argument('-c','--clean', default=False,action="store_true", help='Clean first')
        command_p.add_argument('-l','--library',  help='Name of the library, defined in the config file')
             
        
        #
        # run Command
        #
        command_p = cmd.add_parser('run', help='Run a method on the bundle')
        command_p.set_defaults(command='run')               
        command_p.add_argument('method', metavar='Method', type=str, 
                       help='Name of the mathod to run')    
               
               
        return parser

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

    def update_configuration(self):

        self.config.rewrite(
                         identity=self.identity.to_dict(),
                         partitions=[p.identity.name for p in self.partitions]
                         )
        
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
        import os
        self.rm_rf(self.filesystem.build_path())
        
        mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)
        if os.path.exists(mf):
            os.remove(mf)
        
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

    def pre_meta(self):
        import os.path
        mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)
      
        if os.path.exists(mf):
            self.log("Meta information already generated")
            #raise ProcessError("Bundle has already been prepared")
            return False
        return True

    def meta(self):
        return True
    
    def post_meta(self):
        '''Create the meta marker so we don't run the meta process again'''
        import datetime
        mf = self.filesystem.meta_path(self.META_COMPLETE_MARKER)
        with open(mf,'w+') as f:
            f.write(str(datetime.datetime.now()))
    
        return True


    ### Prepare is run before building, part of the devel process.  

    def pre_prepare(self):
      
        if self.database.exists() and self.db_config.get_value('process','prepared'):
            self.log("Bundle has already been prepared")
            #raise ProcessError("Bundle has already been prepared")
            return False
        return True

    def prepare(self):
        
        if not self.database.exists():
            self.database.create()
        
        return True
    
    def post_prepare(self):
        self.db_config.set_value('process','prepared',True)
        return True
   

    ### Build the final package

    def pre_build(self):
        
        if not self.database.exists():
            raise ProcessError("Database does not exist yet. Was the 'prepare' step run?")
        
        if not self.db_config.get_value('process','prepared'):
            raise ProcessError("Build called before prepare completed")
        return True
        
    def build(self):
        return True
    
    def post_build(self):
        return True
    
        
    ### Submit the package to the library
 
    def pre_install(self):
        return True
    
    def install(self, library_name='default'):  
        '''Install the bundle and all partitions in the default library'''
     
        import databundles.library
     
        library = databundles.library.get_library(name=library_name)
     
        self.log("Install bundle {}".format(self.identity.name))  
        dest = library.put(self)
        self.log("Installed to {} ".format(dest[2]))
        
        for partition in self.partitions:
        
            self.log("Install partition {}".format(partition.name))  
            dest = library.put(partition)
            self.log("Installed to {} ".format(dest[2]))

        return True
        
    def post_install(self):
        return True
    
    ### Submit the package to the repository
 
    def pre_submit(self):
        return True
    
    ### Submit the package to the repository
    def submit(self):
    
        self.repository.submit(root=self.run_args.name, force=self.run_args.force, 
                               repo=self.run_args.repo)
        return True
    
    def post_submit(self):
        return True

    ### Submit the package to the repository
 
    def pre_extract(self):
        return True
    
    ### Submit the package to the repository
    def extract(self):
        self.repository.extract(root=self.run_args.name, force=self.run_args.force)
        return True
    
    def post_extract(self):
        return True
    
    
    ########################
    # Support for the submit() process
 

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

        self._run_config = get_runconfig(self.local_file)
     
   
        # If there is no id field, create it immediately and
        # write the configuration back out. 
   
        if not self._run_config.identity.get('id',False):
            from databundles.identity import DatasetNumber
            self._run_config.identity.id = str(DatasetNumber())
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

    def rewrite(self, **kwargs):
        '''Re-writes the file from its own data. Reformats it, and updates
        the modification time'''
        import yaml
        
        temp = self.local_file+".temp"
        old = self.local_file+".old"
        
        config = AttrDict()
    
        config.update_yaml(self.local_file)
        
        for k,v in kwargs.items():
            config[k] = v
   
        with open(temp, 'w') as f:
            config.dump(f)
    
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
      
        group = bd.get(group)
        
        if not group:
            return None
        
        
        return group

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

    def get_value(self, group, key):
        
        group = self.group(group)
        
        if not group:
            return None
        
        return group.__getattr__(key)

    def get_dataset(self):
        '''Initialize the identity, creating a dataset record, 
        from the bundle.yaml file'''
        
        from databundles.orm import Dataset
 
        s = self.database.session

        return  (s.query(Dataset).one())

   


    