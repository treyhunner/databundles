"""Rest interface for accessing a remote library. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from databundles.client.siesta  import API 
import databundles.client.exceptions 


class NotFound(Exception):
    pass

class RestError(Exception):
    pass

def raise_for_status(response):
    import pprint

    e = databundles.client.exceptions.get_exception(response.status)
        
    if e:
        raise e(response.message)
    

class Rest(object):
    '''Interface class for the Databundles Library REST API
    '''

    def __init__(self, url):
        '''
        '''
        
        self.url = url
        
    @property
    def api(self):
        # It would make sense to cache self.api = API)(, but siesta saves the id
        # ( calls like api.datasets(id).post() ), so we have to either alter siesta, 
        # or re-create it every call. 
        return API(self.url)
        
    def get(self, id_or_name, file_path=None):
        '''Get a bundle by name or id and either return a file object, or
        store it in the given file object
        
        Args:
            id_or_name 
            file_path A string or file object where the bundle data should be stored
                If not provided, the method returns a remose object, from which the
                caller mys read the body
        
        return
        
        '''
        response  = self.api.dataset(id_or_name).bundle.get()
  
        if response.status == 404:
            raise NotFound("Didn't find a file for {}".format(id_or_name))
        elif response.status != 200:
            raise RestError("Error from server: {} {}".format(response.status, response.reason))
  
        if file_path:
            with open(file_path,'w') as file_:
                chunksize = 8192
                chunk =  response.read(chunksize) #@UndefinedVariable
                while chunk:
                    file_.write(chunk)
                    chunk =  response.read(chunksize) #@UndefinedVariable
    
            return file_path
        else:
            # Read the damn thing yourself ... 
            return response
            
    def _put(self, id_,source):
        '''Put the source to the remote, creating a compressed version if
        it is not originally compressed'''
        
        from databundles.util import bundle_file_type
        import gzip
        import os, tempfile, uuid
 
        type_ = bundle_file_type(source)
        
        if  type_ == 'sqlite':
            # If it is a plain sqlite file, compress it before sending it. 
            try:
                cf = os.path.join(tempfile.gettempdir(),str(uuid.uuid4()))
                f = gzip.open(cf, 'wb')
                f.writelines(source)
                f.close()
             
                with open(cf) as source:
                    response =  self.api.datasets(id_).put(source)

            finally:
                if os.path.exists(cf):
                    os.remove(cf)
       
        elif type_ == 'gzip':
            # the file is already gziped, so nothing to do. 
            response =  self.api.datasets(id_).put(source)
        else:
            raise Exception("Bad file")

        raise_for_status(response)
        
        return response
        

    def put(self,id_,source):
        '''Put the bundle in source to the remote library 
        Args:
            source. Either the name of the bundle file, or a file-like opbject
        '''
        
        try:
            # a Filename
            with open(source) as flo:
                r =  self._put(id_,flo)
        except:
            # an already open file
            r =  self._put(id_,source)
            
        raise_for_status(r)
        
        return r
            
   
    def find(self, query):
        '''Find datasets, given a QueryCommand object'''
        
        from collections import namedtuple
        Ref = namedtuple('Ref','Dataset Partition')
        Entry = namedtuple('Entry','id_ name')
        
        response =  self.api.datasets.find.post(query.to_dict())
        raise_for_status(response)
        
        # Convert the result back to the form we get from the Library query 
        return [ Ref(Entry(i['dataset']['id_'], i['dataset']['name']) ,
                     Entry(i['partition']['id_'], i['partition']['name'])  if i['partition'] else None) 
                      for i in response.object ]
    
    
    def datasets(self):
        '''Return a list of all of the datasets in the library'''
        response =   self.api.datasets.get()
        raise_for_status(response)
        return response.object
            
    def close(self):
        '''Close the server. Only used in testing. '''
        response =   self.api.test.closeget()
        raise_for_status(response)
        return response.object

    
    

    
        