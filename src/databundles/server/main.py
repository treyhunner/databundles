'''
REST Server For DataBundle Libraries. 
'''

from bottle import  run, get, put, post, request, static_file #@UnresolvedImport

import databundles.library 
import databundles.run
from databundles.bundle import DbBundle

library = databundles.library.get_library()


@get('/config')
def get_config():
    '''Return all of the dataset identities, as a dict, 
    indexed by id'''
    rc = databundles.run.RunConfig().dict
    return rc

@get('/datasets')
def get_datasets():
    '''Return all of the dataset identities, as a dict, 
    indexed by id'''
    return { i.id_ : i.to_dict() for i in library.dataset_ids}
    

@post('/datasets')
def post_dataset(): 
    '''Store a bundle, calling put() on the bundle file in the Library'''
    import uuid # For a random filename. 
    import io
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
    dataset,library_path = library.put(DbBundle(cf))
    
    # if that worked, OK to remove the temporary file. 
    os.remove(cf)
    
    r = {'dataset':{'id':dataset.id_, 'name':dataset.name}, 
            'path': library_path}

    return r

    
@post('/datasets/find')
def post_datasets_find():
    ''' '''
    return post_datasets_find.__doc__

def get_dataset_record(id):
    ds =  library.findByIdentity(id)
    
    if len(ds) == 0:
        return None
        
    if len(ds) > 1:
        raise Exception("Got more than one result")
    

    return ds.pop()

@get('/dataset/<did>')    
def get_dataset_info(did):
    '''Return a single dataset given an id_ or name'''
    
    return get_dataset_record(did).identity.to_dict()

@get('/dataset/:did/bundle')
def get_dataset_bundle(did):
    '''Get a bundle database file. 
    
    Args:
        id    The Name or id of the dataset bundle. 
              May be for a bundle or partition
    
    '''
    
    bp = library.get(did)
    
    print "FILE", bp.database.path
    
    return static_file(bp.database.path, root='/', mimetype="application/octet-stream")



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

@get('/partition/:pid/table/:tid/data')
def get_partition_data(pid,tid):
    pass



run(host='localhost', port=8080, reloader=True)
