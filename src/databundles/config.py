'''
Created on Jun 23, 2012

@author: eric
'''
import os.path
from databundles.filesystem import Filesystem

from exceptions import  ConfigurationError


class Config(object):
    '''Binds configuration items to the database, and processes the bundle.yaml file'''
    
    BUNDLE_CONFIG_FILE = 'bundle.yaml'

    def __init__(self, bundle, directory):
        '''Maintain link between bundle.yam file and Config record in database'''
        
        self.bundle = bundle
        
        if not directory or not os.path.isdir(directory):
            raise ConfigurationError("Is not a directory: "+str(directory))
        
        config_file = self.bundle.filesystem.path(Config.BUNDLE_CONFIG_FILE)
        
        if not os.path.exists(config_file):
            raise ConfigurationError("Can't find bundle config file: "+config_file)
        
        self.config_file = config_file
        self.directory = directory

        bfr = self.filerec(Config.BUNDLE_CONFIG_FILE, True)
             

        if bfr._is_new or self.file_changed(bfr):
            self.get_or_new_dataset(delete=True)
            self.reload_config()
            self.bundle.filesystem.ref(Config.BUNDLE_CONFIG_FILE).update()
            
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
         
        # Delete all of the records for this dataset that have the
        # bundle.yaml file as a source. 
        (s.query(SAConfig)
         .filter(SAConfig.d_id == ds.id_)
         .filter(SAConfig.source == Config.BUNDLE_CONFIG_FILE )
         .delete())
        
         
        for group,gvalues in self.config_dict.items():
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
                               source=Config.BUNDLE_CONFIG_FILE,
                               d_id=ds.id_,
                               value = value
                               )
                    s.add(o)
        s.commit()

    
    
    def file_changed(self,filerec):
        a_path = self.bundle.filesystem.path(filerec.path)
        
        t1 = filerec.modified
        t2 = os.path.getmtime(a_path)
     
        if  t2 > t1:
            if Filesystem.file_hash(a_path) != filerec.content_hash:
                return True
            else:
                return False
        else:
            return False
            
    
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
            c = self.config_dict
           
            ds = Dataset(**c['identity'])
            s.add(ds)
            s.commit()

            # Re-write the bundle.yaml file with the dataset id. 
            if not c.get('identity',False) or not c.get('identity').get('id',False):
                import yaml
                c['identity']['id'] = ds.id_.encode('ascii','ignore')
                yaml.dump(c, file(self.config_file, 'w'), 
                          indent=4, default_flow_style=False)
            
                bfr = self.bundle.filesystem.ref(Config.BUNDLE_CONFIG_FILE)
              
                bfr.update()

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
            

         
            