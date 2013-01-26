'''
REST Server For DataBundle Libraries. 
'''

from bottle import  get, put, post, request, response #@UnresolvedImport
from bottle import HTTPResponse, static_file, install #@UnresolvedImport
from bottle import ServerAdapter, server_names #@UnresolvedImport

from decorator import  decorator #@UnresolvedImport
import databundles.library 
import databundles.run
import databundles.util
from databundles.bundle import DbBundle


# This might get changed, as in test_run
run_config = databundles.run.RunConfig()

logger = databundles.util.get_logger(__name__)

def get_library_config(name='default'):
    return run_config.library.get(name)
    
def get_library(name='default'):
    '''Return the library. In a function to defer execution, so the
    run_config variable can be altered before it is called. '''

    l = databundles.library.get_library(run_config, name)
    
    return l
    

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
    return { i.id_ : i.to_dict() for i in get_library().datasets}
    
@post('/datasets')
@CaptureException
def post_dataset(): 
    '''Store a bundle, calling put() on the bundle file in the Library.
    '''
    import uuid # For a random filename. 
    import os, tempfile
    import zlib
   
    r = {
             'path': 'none',
             'dataset':{'id': 'none', 'name': 'none' }, 
             'partition' : 'none'
        }
   
    compressed = False #@UnusedVariable
    try:
        compressed  = bool(int(request.query.compressed)) #@UnusedVariable
    except: pass

    try:
        cf = os.path.join(tempfile.gettempdir(),'rest-downloads',str(uuid.uuid4()))
        if not os.path.exists(os.path.dirname(cf)):
            os.makedirs(os.path.dirname(cf))
            
        # Really important to only call request.body once! The property method isn't
        # idempotent!
        body = request.body # Property acessor
        
        # This method can recieve data as compressed or not, and determines which
        # from the magic number in the head of the data. 
        data_type = databundles.util.bundle_file_type(body)
        decomp = zlib.decompressobj(16+zlib.MAX_WBITS) # http://stackoverflow.com/a/2424549/1144479
     
        if not data_type:
            raise Exception("Bad data type: not compressed nor sqlite")
     
        # Read the file directly from the network, writing it to the temp file,
        # and uncompressing it if it is compressesed. 
        with open(cf,'w') as f:

            chunksize = 8192
            chunk =  body.read(chunksize) #@UndefinedVariable
            while chunk:
                if data_type == 'gzip':
                    f.write(decomp.decompress(chunk))
                else:
                    f.write(chunk)
                chunk =  body.read(chunksize) #@UndefinedVariable
        
        # Now we have the bundle in cf. Stick it in the library. 
        
        # Is this a partition or a bundle?
        tb = DbBundle(cf)
     
        dataset, partition, library_path = get_library().put(tb)
        
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
    except Exception as e:
        logger.error("Exception: {} ".format(e))
        raise

@post('/datasets/find')
def post_datasets_find():
    '''This is the doc'''
    from databundles.library import QueryCommand
   
    q = request.json
   
    bq = QueryCommand(q)
    db_query = get_library().find(bq)
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
    ds =  get_library().findByIdentity(id_)
    
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
@CaptureException
def get_dataset_bundle(did):
    '''Get a bundle database file, given an id or name
    
    Args:
        id    The Name or id of the dataset bundle. 
              May be for a bundle or partition
    
    '''
    
    bp = get_library().get(did)

    if bp is False:
        raise Exception("Didn't file dataset for id: {} ".format(did))

    return static_file(bp.bundle.database.path, root='/', mimetype="application/octet-stream")

@get('/dataset/:did/info')
def get_dataset_info(did):
    '''Return the complete record for a dataset, including
    the schema and all partitions. '''

@get('/dataset/<id_>/partitions')
def get_dataset_partitions_info(id_):
    ''' GET    /dataset/:id_/partitions''' 
    ds =  get_library().findByIdentity(id_)
    if len(ds) == 0:
        return None
        
    if len(ds) > 1:
        raise Exception("Got more than one result")
    
    out = {}

    for partition in get_dataset_record(id_).partitions:
        out[partition.id_] = partition.to_dict()
        
    return out;

@post('/dataset/<id_>/partitions')
def post_dataset_partitions_info(id_):
    ''' GET    /dataset/:id_/partitions''' 
    ds =  get_library().findByIdentity(id_)
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

@get('/test/close')
def get_test_close():
    '''Close the server'''
    global stoppable_wsgi_server_run
    if stoppable_wsgi_server_run is not None:
        print "SERVER CLOSING"
        stoppable_wsgi_server_run = False
        return 'should be closed'
    
    else:
        return "not in debug mode. won't close"


class StoppableWSGIRefServer(ServerAdapter):
    '''A server that can be stopped by setting the module variable
    stoppable_wsgi_server_run to false. It is primarily used for testing. '''
    
    def run(self, handler): # pragma: no cover
        global stoppable_wsgi_server_run
        stoppable_wsgi_server_run = True
   
        from wsgiref.simple_server import make_server, WSGIRequestHandler
        if self.quiet:
            class QuietHandler(WSGIRequestHandler):
                def log_request(*args, **kw): pass #@NoSelf
            self.options['handler_class'] = QuietHandler
        srv = make_server(self.host, self.port, handler, **self.options)
        while stoppable_wsgi_server_run:
            srv.handle_request()

server_names['stoppable'] = StoppableWSGIRefServer

def test_run(config=None):
    '''Run method to be called from unit tests'''
    from bottle import run, debug
  
    # Reset the library  with a  different configuration. This is the module
    # level library, defined at the top of the module. 
    if config:
        global run_config
        run_config = config # If this is called before get_library, will change the lib config

    debug()

    l = get_library()  # fixate library
    config = get_library_config()
    port = config.get('port', 7979)
    host = config.get('host', 'localhost')
    return run(host=host, port=port, reloader=False, server='stoppable')

def local_run():
    from bottle import run
    l = get_library()  #@UnusedVariable
    config = get_library_config()
    port = config.get('port', 8080)
    host = config.get('host', '0.0.0.0')
    return run(host=host, port=port, reloader=True)
    
def local_debug_run():
    from bottle import run, debug

    debug()
    l = get_library()  #@UnusedVariable
    config = get_library_config()
    port = config.get('port', 8080)
    host = config.get('host', '0.0.0.0')
    return run(host=host, port=port, reloader=True)
    
if __name__ == '__main__':
    local_debug_run()
    

