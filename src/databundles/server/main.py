'''
REST Server For DataBundle Libraries. 
'''

from bottle import  run, get, put, post, request, response #@UnresolvedImport
from bottle import HTTPResponse, static_file, install #@UnresolvedImport

import databundles.library 
import databundles.run
from databundles.bundle import DbBundle
from decorator import  decorator #@UnresolvedImport

library = databundles.library.get_library()

def make_exception_response(e):
    
    import sys
    import traceback
    
    (exc_type, exc_value, exc_traceback) = sys.exc_info() #@UnusedVariable
    
    tb_list = traceback.format_list(traceback.extract_tb(sys.exc_info()[2]))
    
    return {'exception':
     {'class':e.__class__.__name__, 
      'args':e.args,
      'trace': "\n".join(tb_list)
     }
    }   

def _CaptureException(f, *args, **kwargs):
    '''Decorator implementation for capturing exceptions '''
    try:
        r =  f(*args, **kwargs)
    except Exception as e:
        r = make_exception_response(e)
    
    return r

def CaptureException(f, *args, **kwargs):
    '''Decorator to capture exceptions and convert them
    to a dict that can be returned as JSON ''' 

    return decorator(_CaptureException, f) # Preserves signature


class AllJSONPlugin(object):
    '''A copy of the bottle JSONPlugin, but this one tries to convert
    all objects to json ''' 
    
    from json import dumps as json_dumps
    
    name = 'json'
    api  = 2

    def __init__(self, json_dumps=json_dumps):
        self.json_dumps = json_dumps

    def apply(self, callback, context):
      
        dumps = self.json_dumps
        if not dumps: return callback
        def wrapper(*a, **ka):
            rv = callback(*a, **ka)

            if isinstance(rv, HTTPResponse ):
                return rv
            
            #Attempt to serialize, raises exception on failure
            try:
                json_response = dumps(rv)
            except Exception as e:
                r =  make_exception_response(e)
                json_response = dumps(r)
                
            #Set content type only if serialization succesful
            response.content_type = 'application/json'
            return json_response
        return wrapper

install(AllJSONPlugin())

@get('/datasets')
def get_datasets():
    '''Return all of the dataset identities, as a dict, 
    indexed by id'''
    return { i.id_ : i.to_dict() for i in library.dataset_ids}
    
@post('/datasets')
def post_dataset(): 
    '''Store a bundle, calling put() on the bundle file in the Library'''
    import uuid # For a random filename. 
    import os
    
    cf = library.cache_path('downloads',str(uuid.uuid4()))
    
    # Read the file directly from the network, writing it to the temp file
    with open(cf,'w') as f:

        # Really important to only call request.body once! The property method isn't
        # idempotent!
        body = request.body
        chunksize = 8192
        chunk =  body.read(chunksize) #@UndefinedVariable
        while chunk:
            f.write(chunk)
            chunk =  body.read(chunksize) #@UndefinedVariable
    
    # Now we have the bundle in cf. Stick it in the library. 
    
    # Is this a partition or a bundle?
    tb = DbBundle(cf)
    
    force_partition = (tb.db_config.info.type == 'partition')
    remove = (tb.db_config.info.type != 'partition')
        
    dataset, partition, library_path = library.put(tb, remove=remove, force_partition=force_partition)
    
    # if that worked, OK to remove the temporary file. 
    os.remove(cf)

    if partition:
        partition = {'id':partition.identity.id_, 'name':partition.name}
    else:
        partition = None 
        
    r = {
         'path': library_path,
         'dataset':{'id':dataset.id_, 'name':dataset.name}, 
         'partition' : partition
         }
        

    return r

@post('/datasets/find')
def post_datasets_find():
    '''This is the doc'''
   
    q = request.json
   
    bq = library.query(q)
    db_query = library.find(bq)
    results = db_query.all() #@UnusedVariable
  
    out = []
    for r in results:
        if isinstance(r, tuple):
            e = { 'dataset': {'id_': r.Dataset.id_, 'name': r.Dataset.name} ,
                  'partition' : {'id_': r.Partition.id_, 'name': r.Partition.name}
                 
                 }
        else:
            e = { 'dataset': {'id_': r.Dataset.id_, 'name': r.Dataset.name},
                  'partition': None
                 }
  
        out.append(e)
        
        return out
  
   

def get_dataset_record(id_):
    ds =  library.findByIdentity(id_)
    
    if len(ds) == 0:
        return None
        
    if len(ds) > 1:
        raise Exception("Got more than one result")

    return ds.pop()

@get('/dataset/<did>')    
def get_dataset_identity(did):
    '''Return a single dataset identity record given an id_ or name.
    Returns only the dataset record, excluding chld objects like partitions, 
    tables, and columns. 
    
    '''
    
    return get_dataset_record(did).identity.to_dict()

@get('/dataset/:did/bundle')
def get_dataset_bundle(did):
    '''Get a bundle database file, given an id or name
    
    Args:
        id    The Name or id of the dataset bundle. 
              May be for a bundle or partition
    
    '''
    
    bp = library.get(did)
 
    return static_file(bp.database.path, root='/', mimetype="application/octet-stream")

@get('/dataset/:did/info')
def get_dataset_info(did):
    '''Return the complete record for a dataset, including
    the schema and all partitions. '''

@get('/dataset/<id_>/partitions')
def get_dataset_partitions_info(id_):
    ''' GET    /dataset/:id_/partitions''' 
    ds =  library.findByIdentity(id_)
    if len(ds) == 0:
        return None
        
    if len(ds) > 1:
        raise Exception("Got more than one result")
    
    out = {}

    for partition in get_dataset_record(id_).partitions:
        out[partition.id_] = partition.to_dict()
        
    return out;

#### Test Code

@get('/test/echo/<arg>')
def get_test(arg):
    '''just echo the argument'''
    return  (arg, dict(request.query.items()))

@put('/test/echo')
def put_test():
    '''just echo the argument'''
    return  (request.json, dict(request.query.items()))


@get('/test/exception')
@CaptureException
def get_test_exception():
    '''Throw an exception'''
    raise Exception("throws exception")



run(host='localhost', port=8080, reloader=True)
