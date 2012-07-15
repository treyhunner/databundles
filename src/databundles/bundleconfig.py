'''
Created on Jun 23, 2012

@author: eric
'''
import os.path
from databundles.filesystem import Filesystem
from databundles.identity import Identity
from exceptions import  ConfigurationError

class AttrDict(dict):
    """A dictionary with attribute-style access. It maps attribute access to
    the real dictionary.  """
 
    def __init__(self, base):
        setattr(self, '_base', base)  
 
class BundleConfig(object):
   
    def __init__(self):
        pass


class BundleFileConfig(BundleConfig):
    '''Represents the bundle configuration file and handled access to it. '''
    
    BUNDLE_CONFIG_FILE = 'bundle.yaml'

    def __init__(self, directory):
        '''Load the bundle.yaml file and create a config object
        
        If the 'id' value is not set in the yaml file, it will be created and the
        file will be re-written
        '''

        super(BundleFileConfig, self).__init__()
        
        self.directory = directory
     
        self._config_dict = None
        self.dict # Fetch the dict. 
   
        # If there is no id field, create it immediately and
        # write the configuration baci out. 
   
        if not self.identity.id_:
            from objectnumber import DatasetNumber
            self.identity.id_ = str(DatasetNumber())
            self.rewrite()
   
        if not os.path.exists(self.path):
            raise ConfigurationError("Can't find bundle config file: "+self.config_file)

        
    @property
    def dict(self): #@ReservedAssignment
        '''Return a dict/array object tree for the bundle configuration'''
        
        if not self._config_dict:  
            import yaml
            try:
                self._config_dict =  yaml.load(file(self.path, 'r')) 
    
            except:
                raise NotImplementedError,''' Bundle.yaml missing. 
                Auto-creation not implemented'''
            
        return self._config_dict

    def __getattr__(self, group):
        '''Fetch a confiration group and return the contents as an 
        attribute-accessible dict'''
        
        inner = self.dict[group]
        
        class attrdict(object):
            def __setattr__(self, key, value):
                key = key.strip('_')
                inner[key] = value

            def __getattr__(self, key):
                key = key.strip('_')
                if key not in inner:
                    return None
                
                return inner[key]
        
        return attrdict()

    @property
    def path(self):
        return os.path.join(self.directory, BundleFileConfig.BUNDLE_CONFIG_FILE)

    def reload(self): #@ReservedAssignment
        '''Reload the configuation from the file'''
        self._config_dict = None
        
    def rewrite(self):
        '''Re-writes the file from its own data. Reformats it, and updates
        themodification time'''
        import yaml
        
        yaml.dump(self.dict, file(self.path, 'w'), indent=4, default_flow_style=False)

        
class BundleDbConfig(BundleConfig):
    '''Binds configuration items to the database, and processes the bundle.yaml file'''

    def __init__(self, database):
        '''Maintain link between bundle.yam file and Config record in database'''
        
        super(BundleDbConfig, self).__init__()
        self.database = database
        self.dataset = self.get_dataset()

    @property
    def dict(self): #@ReservedAssignment
        '''Return a dict/array object tree for the bundle configuration'''
      
        return {'identity':self.dataset.to_dict()}

    def __getattr__(self, group):
        '''Fetch a confiration group and return the contents as an 
        attribute-accessible dict'''
        
        inner = self.dict[group]
        
        class attrdict(object):
            def __setattr__(self, key, value):
                key = key.strip('_')
                inner[key] = value

            def __getattr__(self, key):
                key = key.strip('_')
                if key not in inner:
                    return None
                
                return inner[key]
        
        return attrdict()

    def get_dataset(self):
        '''Initialize the identity, creating a dataset record, 
        from the bundle.yaml file'''
        
        from databundles.orm import Dataset
 
        s = self.database.session

        return  (s.query(Dataset).one())

    #################


    def get_url(self,source_url, create=False):
        '''Return a database record for a file'''
    
        from databundles.orm import File
        import sqlalchemy.orm.exc
 
        s = self.database.session
        
        try:
            o = (s.query(File).filter(File.source_url==source_url).one())
         
        except sqlalchemy.orm.exc.NoResultFound:
            if create:
                o = File(source_url=source_url,path=source_url,process='none' )
                s.add(o)
                s.commit()
            else:
                return None
          
          
        o.session = s # Files have SavableMixin
        return o
    
    def get_or_new_url(self, source_url):
        return self.get_url(source_url, True)
 
    def add_file(self, rel_path):
        return self.filerec(rel_path, True)
    
    def filerec(self, rel_path, create=False):
        '''Return a database record for a file'''
    
        from databundles.orm import File
        import sqlalchemy.orm.exc

        s = self.database.session
        
        if not rel_path:
            raise ValueError('Must supply rel_path')
        
        try:
            o = (s.query(File).filter(File.path==rel_path).one())
            o._is_new = False
        except sqlalchemy.orm.exc.NoResultFound as e:
           
            if not create:
                raise e
           
            a_path = self.filesystem.path(rel_path)
            o = File(path=rel_path,
                     content_hash=Filesystem.file_hash(a_path),
                     modified=os.path.getmtime(a_path),
                     process='none'
                     )
            s.add(o)
            s.commit()
            o._is_new = True
            
        except Exception as e:
            return None
        
        return o

            