'''
Created on Jun 9, 2012

@author: eric
'''

from database import Database
from identity import Identity
from partition import Partition
from filesystem import  Filesystem
from config import Config
from schema import Schema
from partition import Partitions

class Bundle(object):
    '''Represents a bundle, including all configuration 
    and top level operations. '''
 
    def __init__(self, directory=None):
        '''Initialize a bundle and all of its sub-components. 
        
        If it does not exist, creates the bundle database and initializes the
        Dataset record and COnfig records from the bundle.yaml file. Through the
        config object, will trigger a re-load of the bundle.yaml file if it
        has changed. 
        
        Order of operations is:
            Create bundle.db if it does not exist
        '''
        
        if not directory:
            directory = Filesystem.find_root_dir()
        
        self.filesystem = Filesystem(self, directory)
        self.database = Database(self)
        
        self.config = Config(self,directory)
        self.identity = Identity(self)
        self.schema = Schema(self)
        self.partitions = Partitions(self)

    def log(self, message, **kwargs):
        '''Log the messsage'''
        print "LOG: ",message

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
   
    ### Download URLS from a list, hand coded, or from prepare() 
   
    def pre_download(self):
        return True  
    
    def download(self):
        return True  
    
    def post_download(self):
        return True  
    
    ### Transform to the database format
 
    def pre_transform(self):
        return True
    
    def transform(self):
        return True
    
    def post_transform(self):
        return True
    
    ### Build the final package

    def pre_build(self):
        return True
        
    def build(self):
        return True
    
    def post_build(self):
        return True
    
        
    ### Submit the package to the repository
 
    def pre_submit(self):
        return True
    
    def submit(self):
        return True
        
    def post_submit(self):
        return True
    
    
    
    