'''
Rest interface for accessing a remote library. 

Created on Aug 31, 2012

@author: eric
'''
from siesta  import API
from databundles.library import BundleQueryCommand
from databundles.bundle import DbBundle

class NotFound(Exception):
    pass

class Rest(object):
    '''
    classdocs
    '''

    def __init__(self, url):
        '''
        
        '''
        
        self.url = url
        self.api = API(self.url)
        
    def get(self, id_or_name, file_):
        '''Get a bundle by name or id and either return a file object, or
        store it in the given file object
        
        Args:
            file_ A string or file object where the bundle data should be stored
        
        '''
        resource, response  = self.api.dataset(id_or_name).bundle.get()
  
        if response.status != 200:
            raise NotFound("Didn't file a file for {}".format(id_or_name))
  
            chunksize = 8192
            chunk =  response.read(chunksize) #@UndefinedVariable
            while chunk:
                file_.write(chunk)
                chunk =  response.read(chunksize) #@UndefinedVariable

        return response.getheaders()
        
    def put(self,o):
        resource, response = self.api.datasets.post(o)
        
        return  resource.attrs
    
    def query(self):
        '''Return a query object to use in find()'''
        return  BundleQueryCommand()
    
    def find(self, query):
        pass
    
    def datasets(self):
        '''Return alist of all of the datasets in the library'''
        return self.api.datasets.get()
        

    
    

    
        