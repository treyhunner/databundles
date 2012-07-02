'''
Created on Jun 23, 2012

@author: eric
'''
import os.path
from databundles.filesystem import Filesystem
from databundles.identity import Identity
from databundles.properties import DbRowProperty 
from exceptions import  ConfigurationError

class BundleConfigIdentity(Identity):
    
    id_ = DbRowProperty("id_",None,ascii=True)
    source = DbRowProperty("source",None)
    dataset = DbRowProperty("dataset",None)
    subset = DbRowProperty("subset",None)
    variation = DbRowProperty("variation",None)
    creator = DbRowProperty("creator",None)
    revision = DbRowProperty("revision",None)
    
    def __init__(self, bundle):
        self.super_ = super(BundleConfigIdentity, self)
        # Don't call super constructor. It will construct from dict. 
        self.bundle = bundle

    @property
    def row(self):
        '''Return the dataset row object for this bundle'''
        from databundles.orm import Dataset
        session = self.bundle.database.session
        return session.query(Dataset).first()

class BundleConfigFile(object):
    
    BUNDLE_CONFIG_FILE = 'bundle.yaml'
    
    '''Represents the bundle configuration file and handled access to it. '''
    
    def __init__(self, directory):
        self.directory = directory
        self.config_file = BundleConfigFile.get_config_path(directory)
        self._config_dict = None
        
        if not os.path.exists(self.config_file):
            raise ConfigurationError("Can't find bundle config file: "+self.config_file)
        
    @property
    def config_dict(self):
        '''Return a dict/array object tree for the bundle configuration'''
        
        if self._config_dict:
            return self._config_dict
        
        import yaml
        try:
            self._config_dict =  yaml.load(file(BundleConfigFile.get_config_path(self.directory), 'r'))  
            return self._config_dict
        except:
            raise NotImplementedError,''' Bundle.yaml missing. 
            Auto-creation not implemented'''

    @classmethod
    def get_config_path(cls,directory):
        return os.path.join(directory, BundleConfigFile.BUNDLE_CONFIG_FILE)
  
    @property
    def identity(self):
        return Identity(**self.config_dict.get('identity'))
  
    def changed(self,filerec):

        t1 = filerec.modified
        t2 = os.path.getmtime(self.config_file)
     
        if  t2 > t1:
            if Filesystem.file_hash(self.config_file) != filerec.content_hash:
                return True
            else:
                return False
        else:
            return False
       
    def reload(self): #@ReservedAssignment
        '''Reload the configuation from the file'''
        self._config_dict = None
        return self.config_dict    
        
    def rewrite(self):
        '''Re-writes the file from its own data. Reformats it, and updates
        themodification time'''
        import yaml
        
        yaml.dump(self.config_dict, file(self.config_file, 'w'), 
          indent=4, default_flow_style=False)

        
class BundleConfig(object):
    '''Binds configuration items to the database, and processes the bundle.yaml file'''

    def __init__(self, bundle):
        '''Maintain link between bundle.yam file and Config record in database'''
        
        self.bundle = bundle
        self._bundle_config_file = None
        if hasattr(self.bundle,'filesystem'):
            self.update_from_file()

    def update_from_file(self):

        config_file = BundleConfigFile(self.bundle.filesystem.path())

        bfr = self.filerec(BundleConfigFile.BUNDLE_CONFIG_FILE, True)
             
        if bfr._is_new or config_file.changed(bfr):
            self.get_or_new_dataset(delete=True)
            from databundles.orm import Config as SAConfig
            import sqlalchemy.orm.exc
            s = self.bundle.database.session
            ds = self.get_or_new_dataset()
             
            # Delete all of the records for this dataset that have the
            # bundle.yaml file as a source. 
            (s.query(SAConfig)
             .filter(SAConfig.d_id == ds.id_)
             .filter(SAConfig.source == BundleConfigFile.BUNDLE_CONFIG_FILE )
             .delete())
            
            for group,gvalues in config_file.config_dict.items():
                for key, value in gvalues.items():
                    try:
                        # This is useless here, btu also harmless. 
                        o = (s.query(SAConfig)
                             .filter(SAConfig.d_id == ds.id_)
                             .filter(SAConfig.group == group)
                             .filter(SAConfig.key == key)
                             .one())
                        
                    except sqlalchemy.orm.exc.NoResultFound:
                       
                        o = SAConfig(
                                   group=group,
                                   key=key,
                                   source=BundleConfigFile.BUNDLE_CONFIG_FILE,
                                   d_id=ds.id_,
                                   value = value
                                   )
                        s.add(o)
            s.commit()

            # Re-add the file ref for the bundle.yaml file so we can track when it
            # changes. 
            self.bundle.filesystem.ref(BundleConfigFile.BUNDLE_CONFIG_FILE).update()
            
        else:
            self.get_or_new_dataset()
     
    def group(self,group):
        '''Extract a set of configuration items from the database and 
        return them as a dict'''
        from databundles.orm import Config as SAConfig
     
        s = self.bundle.database.session
        ds = self.get_or_new_dataset()
      
        q = (s.query(SAConfig)
             .filter(SAConfig.group == group)
             .filter(SAConfig.d_id == ds.id_)
             .all())
         
        gd = {} 
        for o in q:
            gd[o.key] = o.value
         
        return gd;

   
    def get_url(self,source_url, create=False):
        '''Return a database record for a file'''
    
        from databundles.orm import File
        import sqlalchemy.orm.exc
 
        s = self.bundle.database.session
        
        try:
            o = (s.query(File).filter(File.source_url==source_url).one())
         
        except sqlalchemy.orm.exc.NoResultFound:
          
            o = File(source_url=source_url,process='none' )
            s.add(o)
          
          
        o.session = s # Files have SavableMixin
        return o
    
    def get_or_new_url(self, source_url):
        return self.get_url(source_url, True)
    
    def get_or_new_dataset(self, delete = False):
        '''Initialize the identity, creating a dataset record, 
        from the bundle.yaml file'''
        
        from databundles.orm import Dataset
        import sqlalchemy.orm.exc
 
        s = self.bundle.database.session

        try:
            if delete:
             
                s.delete(s.query(Dataset).one())
                ds = None
            else:
                ds = (s.query(Dataset).one())
          
        except sqlalchemy.orm.exc.NoResultFound:
            ds = None
            pass

        if not ds:
            bcf = BundleConfigFile(self.bundle.filesystem.path())
            c = bcf.config_dict
           
            ds = Dataset(**c['identity'])
            ds.name = Identity.name_str(ds)
           
            s.add(ds)
            s.commit()

            # Re-write the bundle.yaml file with the dataset id. 
            if not c.get('identity',False) or not c.get('identity').get('id',False):
                bcf.config_dict['identity']['id'] = ds.id_.encode('ascii','ignore')
                bcf.rewrite()         
                bfr = self.bundle.filesystem.ref(BundleConfigFile.BUNDLE_CONFIG_FILE)
              
                bfr.update()

        return ds

    @property
    def identity(self):
        return BundleConfigIdentity(self.bundle)
      
    def config_file_changed(self):
        return self.config_file.changed(self.filerec(BundleConfigFile.BUNDLE_CONFIG_FILE, True))
   
    def add_file(self, rel_path):
        return self.filerec(rel_path, True)
    
    def filerec(self, rel_path, create=False):
        '''Return a database record for a file'''
    
        from databundles.orm import File
        import sqlalchemy.orm.exc
 
        s = self.bundle.database.session
        
        if not rel_path:
            raise ValueError('Must supply rel_path')
        
        try:
            o = (s.query(File).filter(File.path==rel_path).one())
            o._is_new = False
        except sqlalchemy.orm.exc.NoResultFound as e:
           
            if not create:
                raise e
           
            a_path = self.bundle.filesystem.path(rel_path)
            o = File(path=rel_path,
                     content_hash=Filesystem.file_hash(a_path),
                     modified=os.path.getmtime(a_path),
                     process='none'
                     )
            s.add(o)
            s.commit()
            o._is_new = True
            
        return o

    @property
    def config_file(self):
        if not self._bundle_config_file:
            self._bundle_config_file = BundleConfigFile(self.bundle.filesystem.path())
        return self._bundle_config_file
         
            