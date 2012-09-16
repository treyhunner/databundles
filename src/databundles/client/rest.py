'''
Rest interface for accessing a remote library. 

Created on Aug 31, 2012

@author: eric
'''
from siesta  import API #@UnresolvedImport
from databundles.library import BundleQueryCommand
from databundles.bundle import DbBundle

class NotFound(Exception):
    pass

class RestError(Exception):
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
        
    def get(self, id_or_name, file_path):
        '''Get a bundle by name or id and either return a file object, or
        store it in the given file object
        
        Args:
            file_ A string or file object where the bundle data should be stored
        
        '''
        response  = self.api.dataset(id_or_name).bundle.get()
  
        if response.status == 404:
            raise NotFound("Didn't find a file for {}".format(id_or_name))
        elif response.status != 200:
            raise RestError("Error from server: {} {}".format(response.status, response.reason))
  
        with open(file_path,'w') as file_:
            chunksize = 8192
            chunk =  response.read(chunksize) #@UndefinedVariable
            while chunk:
                file_.write(chunk)
                chunk =  response.read(chunksize) #@UndefinedVariable

        return DbBundle(file_path)
            
    def put(self,o):
        response = self.api.datasets.post(o)
        
        return  response
    
    def query(self):
        '''Return a query object to use in find()'''
        return  BundleQueryCommand()
    
    def find(self, query):
        
        from collections import namedtuple
        Ref = namedtuple('Ref','Dataset Partition')
        Entry = namedtuple('Entry','id_ name')
        
        response =  self.api.datasets.find.post(query.to_dict())
        
        # Convert the result back to the form we get from the Library query 
        return [ Ref(Entry(i['dataset']['id_'], i['dataset']['name']) ,
                     Entry(i['partition']['id_'], i['partition']['name'])  if i['partition'] else None) 
                      for i in response.object ]
    
    
    def datasets(self):
        '''Return a list of all of the datasets in the library'''
        response =   self.api.datasets.get()
        return response.object
        

    
    

    
        