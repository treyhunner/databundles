'''
Runtime configuration. 

The RunConfig object will search for a databundles.yaml file in multiple locations, 
including: 

  /etc/databundles.yaml
  ~user/.databundles.yaml
  The current directory
  A named path ( --config option )
  
It will start from the first directory, and for each one, try to load the file
and copy the values into an accumulator, with later values overwritting
earlier ones. 

'''

import yaml
import os
import os.path

class RunConfig(object):
    '''  '''

    def __init__(self, path=None):
        '''Create a new RunConfig object
        
        Arguments
        path -- If present, a yaml file to load last, overwriting earlier values
        '''
        
        self.config = {}
        self.config['loaded'] = []
    
        self.load('/etc/databundles.yaml')
        self.load(os.path.expanduser('~/.databundles.yaml'))
        self.load(os.path.join(os.getcwd(),'databundles.yaml'))
        
        if path is not None:
            self.load(os.path.join(path))
        
    def overlay(self,config):
        '''Overlay the values from an input dictionary into 
        the object configuration, overwritting earlier values. '''
        

        for name,group in config.items():
            
            if not name in self.config:
                self.config[name] = {}
            
            try:
                for key,value in group.items():
                    self.config[name][key] = value
            except:
                # item is not a group
                self.config[name] = group 
    
    def load(self,path):

        if os.path.exists(path):
            self.overlay(yaml.load(file(path, 'r')))
            self.config['loaded'].append(path)
        

    def group(self, name):
        '''return a dict for a group of configuration items.'''
        
        return self.config.get(name,{})