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
       
        if not directory:
            self.root_dir_  = files.root_dir()
        else:
            self.root_dir_ = files.RootDir(directory)

    @property
    def config(self):
        return self.root_dir_.bundle_config

    @property
    def root_dir(self):
        return self.root_dir_

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
    
    
    
    