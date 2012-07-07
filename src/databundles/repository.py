'''
Created on Jun 10, 2012

@author: eric
'''

from library import Library
class Repository(object):
    '''
    classdocs
    '''


    def __init__(self, url, library):
        '''
        Initialize a reference to a repository, by url, linking it to a library. 
        '''
        
        self.url = url
        self.library = library
        
        
    
    