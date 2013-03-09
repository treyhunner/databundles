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
        
    def get_ref(self, id_or_name):
        '''Return a tuple of (rel_path, dataset_identity, partition_identity)
        for an id or name'''

        response  = self.api.datasets.find(id_or_name)
  
        if response.status == 404:
            raise NotFound("Didn't find a file for {}".format(id_or_name))
        elif response.status != 200:
            raise RestError("Error from server: {} {}".format(response.status, response.reason))
        
        return response.object
  
          
    def get(self, id_or_name, file_path=None):
        '''Get a bundle by name or id and either return a file object, or
        store it in the given file object
        
        Args:
            id_or_name 
            file_path A string or file object where the bundle data should be stored
                If not provided, the method returns a response object, from which the
                caller my read the body. If file_path is True, the method will generate
                a temporary filename. 
        
        return
        
        '''
        response  = self.api.datasets(id_or_name).get()
  
        if response.status == 404:
            raise NotFound("Didn't find a file for {}".format(id_or_name))
        elif response.status != 200:
            raise RestError("Error from server: {} {}".format(response.status, response.reason))
  
        if file_path:
            
            if file_path is True:
                    import uuid,tempfile,os
            
                    file_path = os.path.join(tempfile.gettempdir(),'rest-downloads',str(uuid.uuid4()))
                    if not os.path.exists(os.path.dirname(file_path)):
                        os.makedirs(os.path.dirname(file_path))  
               
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
            
    def get_partition(self, d_id_or_name, p_id_or_name, file_path=None):
        '''Get a partition by name or id and either return a file object, or
        store it in the given file object
        
        Args:
            id_or_name 
            file_path A string or file object where the bundle data should be stored
                If not provided, the method returns a response object, from which the
                caller my read the body. If file_path is True, the method will generate
                a temporary filename. 
        
        return
        
        '''
        response  = self.api.datasets(d_id_or_name).partitions(p_id_or_name).get()
  
        if response.status == 404:
            raise NotFound("Didn't find a file for {} / {}".format(d_id_or_name, p_id_or_name))
        elif response.status != 200:
            raise RestError("Error from server: {} {}".format(response.status, response.reason))
  
        if file_path:
            
            if file_path is True:
                    import uuid,tempfile,os
            
                    file_path = os.path.join(tempfile.gettempdir(),'rest-downloads',str(uuid.uuid4()))
                    if not os.path.exists(os.path.dirname(file_path)):
                        os.makedirs(os.path.dirname(file_path))  
               
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
        from databundles.identity import ObjectNumber, DatasetNumber, PartitionNumber
        
        on = ObjectNumber.parse(id_)
 
        if not on:
            raise ValueError("Failed to parse id: '{}'".format(id_))
 
        if not  isinstance(on, (DatasetNumber, PartitionNumber)):
            raise ValueError("Object number '{}' is neither for a dataset nor partition".format(id_))
 
        type_ = bundle_file_type(source)

        if  type_ == 'sqlite':
            # If it is a plain sqlite file, compress it before sending it. 
            try:
                cf = os.path.join(tempfile.gettempdir(),str(uuid.uuid4()))
                f = gzip.open(cf, 'wb')
                f.writelines(source)
                f.close()
             
                with open(cf) as source:
                    if isinstance(on,DatasetNumber ):
                        response =  self.api.datasets(id_).put(source)
                    else:
                        response =  self.api.datasets(str(on.dataset)).partitions(str(on)).put(source)

            finally:
                if os.path.exists(cf):
                    os.remove(cf)
       
        elif type_ == 'gzip':
            # the file is already gziped, so nothing to do. 

            if isinstance(on,DatasetNumber ):
                response =  self.api.datasets(id_).put(source)
            else:
                response =  self.api.datasets(str(on.dataset)).partitions(str(on)).put(source)
            
        else:
            raise Exception("Bad file got type: {} ".format(type_))


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
        
        if isinstance(r.object, list):
            r.object = r.object[0]

        return r
            
   
    def find(self, query):
        '''Find datasets, given a QueryCommand object'''
        from databundles.library import QueryCommand
        

        if isinstance(query, basestring):
            response =  self.api.datasets.find(query).get()
            raise_for_status(response)
            r = [response.object]
            
        elif isinstance(query, dict):
            # Dict form of  QueryCOmmand
            response =  self.api.datasets.find.post(query)
            raise_for_status(response)
            r = response.object
            
        elif isinstance(query, QueryCommand):
            response =  self.api.datasets.find.post(query.to_dict())
            raise_for_status(response)
            r = response.object
            
        else:
            raise ValueError("Unknown input type: {} ".format(type(query)))
        
        
        raise_for_status(response)
       
    
        # Convert the result back to the form we get from the Library query 
        
        from collections import namedtuple
        Ref1= namedtuple('Ref1','Dataset Partition')
        Ref2= namedtuple('Ref2','Dataset')
        EntryT = namedtuple('Entry','id name source dataset subset variation creator revision time space table grain')
        
        # Allow constructing Entry with missing values
        def Entry(id=None,name=None,source=None,dataset=None,subset=None,
                  variation=None,creator=None,revision=None,
                  time=None, space=None, table=None, grain=None):
            return EntryT(id, name, source, dataset, subset, variation, creator, revision,
                          time,space,table,grain)

        return [ (Ref1(Entry(**i['dataset']) ,Entry(**i['partition'])) if i['partition'] 
                else  Ref2(Entry(**i['dataset']))) for i in r  if i is not False]
    
    
    def list(self):
        '''Return a list of all of the datasets in the library'''
        response =   self.api.datasets.get()
        raise_for_status(response)
        return response.object
            
    def close(self):
        '''Close the server. Only used in testing. '''
        response =   self.api.test.closeget()
        raise_for_status(response)
        return response.object

    
    

    
        