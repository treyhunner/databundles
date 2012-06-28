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
import os.path

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
        
        self.ptick_count = 0;

    def log(self, message, **kwargs):
        '''Log the messsage'''
        print "LOG: ",message

    def progress(self,message):
        '''print message to terminal, in place'''
        print 'PRG: ',message

    def ptick(self,message):
        '''Writes a tick to the stdout, without a space or newline'''
        import sys
        sys.stdout.write(message)
        
        self.ptick_count += 1
       
        if self.ptick_count % 72 == 0:
            sys.stdout.write("\n")

    from contextlib import contextmanager
    @contextmanager
    def extract_zip(self,path):
        '''Context manager to extract a single file from a zip archive, and delete
        it when finished'''
        import zipfile
        '''Extract a the files from a zip archive'''
        
        extractDir = self.filesystem.directory('extracts')

        with zipfile.ZipFile(path) as zf:
            for name in  zf.namelist():
                extractFilename = os.path.join(extractDir,name)
                
                if os.path.exists(extractFilename):
                    os.remove(extractFilename)
                    
                self.log('Extracting'+extractFilename+' from '+path)
                name = name.replace('/','').replace('..','')
                zf.extract(name,extractDir )
                    
                yield extractFilename
                os.unlink(extractFilename)

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
    
    
    
    