'''
Created on Jun 23, 2012

@author: eric
'''
import os.path

from exceptions import  ConfigurationError


class Config(object):
    '''Binds configuration items to the database, and processes the bundle.yaml file'''
    
    BUNDLE_CONFIG_FILE = 'bundle.yaml'

    def __init__(self, bundle, directory):
        '''Maintain link between bundle.yam file and Config record in database'''
        
        self.bundle = bundle
        
        if not os.path.isdir(directory):
            raise ConfigurationError("Is not a directory: "+directory)
        
        config_file = self.bundle.filesystem.path(Config.BUNDLE_CONFIG_FILE)
        
        if not os.path.exists(config_file):
            raise ConfigurationError("Can't find bundle config file: "+config_file)
        
        self.config_file = config_file
        self.directory = directory
        
        self.get_or_new_dataset()
        
        bfr = self.filerec(Config.BUNDLE_CONFIG_FILE)
             
        if bfr._is_new or self.file_changed(bfr):
            self.get_or_new_dataset(delete=True)
            self.reload_config()
     
    def group(self,group):
        '''Extract a set of configuration items from the database and 
        return them as a dict'''
        from databundles.orm import Config as SAConfig
     
        s = self.bundle.database.session
        ds = self.get_or_new_dataset()
      
        q = (s.query(SAConfig)
             .filter(SAConfig.group == group)
             .filter(SAConfig.d_id == ds.oid)
             .all())
         
        gd = {} 
        for o in q:
            gd[o.key] = o.value
         
        return gd;
     
    def reload_config(self):
        '''Reload the configuration values from the bundle.yaml file'''
    
        from databundles.orm import Config as SAConfig
        import sqlalchemy.orm.exc
        s = self.bundle.database.session
        ds = self.get_or_new_dataset()
         
        for group,gvalues in self.config_dict.items():
            for key, value in gvalues.items():
                try:
                    o = (s.query(SAConfig)
                         .filter(SAConfig.group == group)
                         .filter(SAConfig.key == key)
                         .one())
                    
                except sqlalchemy.orm.exc.NoResultFound:
                   
                    o = SAConfig(
                               group=group,
                               key=key,
                               source=Config.BUNDLE_CONFIG_FILE,
                               d_id=ds.oid
                               )
                    s.add(o)
        s.commit()

    def file_hash(self, path):
        '''Compute hash of a file in chuncks'''
        import hashlib
        md5 = hashlib.md5()
        with open(path,'rb') as f: 
            for chunk in iter(lambda: f.read(8192), b''): 
                md5.update(chunk)
        return md5.hexdigest()
    
    def file_changed(self,filerec):
        a_path = self.bundle.filesystem.path(filerec.path)
        
        if os.path.getmtime(a_path) < filerec.modified:
            if self.file_hash(a_path) != filerec.hash:
                return True
            else:
                return False
        else:
            return False
            
    
    def filerec(self, rel_path):
        '''Return a database record for a file'''
    
        from databundles.orm import File
        import sqlalchemy.orm.exc
 
        s = self.bundle.database.session
        
        try:
            o = (s.query(File).filter(File.path==rel_path).one())
            o._is_new = False
        except sqlalchemy.orm.exc.NoResultFound:
            a_path = self.bundle.filesystem.path(rel_path)
            o = File(path=rel_path,
                     content_hash=self.file_hash(a_path),
                     modified=os.path.getmtime(a_path),
                     process='none'
                     )
            s.add(o)
            s.commit()
            o._is_new = True
            
        return o
    
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
            c = self.config_dict
           
            ds = Dataset(**c['identity'])
            s.add(ds)
            s.commit()

        return ds

    @property
    def config_dict(self):
        '''Return a dict/array object tree for the bundle configuration'''
     
        import yaml

        try:
            return  yaml.load(file(self.config_file, 'r'))  
        except:
            raise NotImplementedError,''' Bundle.yaml missing. 
            Auto-creation not implemented'''