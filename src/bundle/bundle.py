'''
Created on Jun 9, 2012

@author: eric
'''

import files

class Bundle(object):
    '''
    classdocs
    '''

    def __init__(self, directory=None):
        '''
        Constructor
        '''
        self.config = None
        
        if not directory:
            root_dir = files.root_dir
        else:
            root_dir = files.RootDir(directory)
            
        self.root_dir = root_dir
            
        self.config = root_dir.bundle_config.yaml
        
    @property
    def config(self):
        return self.root_dir.bundle_config
