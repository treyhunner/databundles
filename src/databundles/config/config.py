'''
Created on Jun 7, 2012

@author: eric
'''

class Config(object):
    '''
    classdocs
    '''
    
    def __init__(self, configFile):
        '''
        Constructor
        '''
        self._configFile = configFile
        self._yaml = None
        self.load()
        
    def load(self):
        import yaml
        self._yaml = yaml.load(file(self._configFile, 'r'))
      
        return self._yaml
    
    
    @property
    def yaml(self):
        """I'm the 'yaml' property."""
        return self._yaml