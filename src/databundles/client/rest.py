'''
Rest interface for accessing a remote library. 

Created on Aug 31, 2012

@author: eric
'''
from siesta  import API

class Rest(object):
    '''
    classdocs
    '''

    def __init__(self, url):
        '''
        
        '''
        
        self.url = url
        self.api = API(self.url)
        
    def put(self, bundle):
        '''Send the bundle to the remote server and store it in the database.'''
        pass
    
    def get(self, id_or_name, file_=None):
        '''Get a bundle by name or id and either return a file object, or
        store it in the given file object
        
        Args:
            file_ A string or file object where the bundle data should be stored
        
        '''
        pass
    
    def find(self, bundle):
        pass
    
    def datasets(self):
        return self.api.datasets.get()
        
        
    def config(self):
        return self.api.config.get()
    
    
    def test_put(self,o):
        return self.api.test.put(o)
    
        