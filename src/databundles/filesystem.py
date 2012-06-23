'''
Created on Jun 23, 2012

@author: eric
'''

import os
import os.path

class Filesystem(object):
    
    BUILD_DIR = 'build'
    
    def __init__(self, bundle, root_directory = None):
        self.bundle = bundle
        if root_directory:
            self.root_directory = root_directory
        else:
            self.root_directory = Filesystem.find_root_dir()
 
        if not os.path.exists(self.path(Filesystem.BUILD_DIR)):
            os.makedirs(self.path(Filesystem.BUILD_DIR),0755)
 
    @staticmethod
    def find_root_dir(testFile='bundle.yaml'):
        '''Find the parent directory that contains the bundle.yaml file '''
        import sys

        d = sys.path[0]
        
        while os.path.isdir(d) and d != '/':
            test =  os.path.normpath(d+'/'+testFile)
            print "D "+test
            if(os.path.isfile(test)):
                return d
            d = os.path.dirname(d)
             
        return None
    
    @property
    def root_dir(self):
        '''Returns the root directory of the bundle '''
        return self.root_directory

    def path(self, *args):
        '''Resolve a path that is relative to the bundle root into an 
        absoulte path'''
     
        return os.path.normpath(self.root_directory+'/'+os.path.join(*args))    

    def directory(self, rel_path):
        '''Resolve a path that is relative to the bundle root into 
        an absoulte path'''
        abs_path = self.path(rel_path)
        if(not os.path.isdir(abs_path) ):
            os.makedirs(abs_path)
        return abs_path
 
 