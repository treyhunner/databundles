'''
REST Server For DataBundle Libraries. 
'''

from bottle import  run, get, post #@UnresolvedImport

from databundles.library import LocalLibrary 

library = LocalLibrary()


@get('/datasets')
def get_datasets():
    '''Return all of the dataset identities, as a dict, 
    indexed by id'''
    return { i.id_ : i.to_dict() for i in library.datasets}
    
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

@get('/dataset/<id>')    
def get_dataset_info( id):
    '''Return a single dataset given an id or name'''
    
    return get_dataset_record(id).identity.to_dict()

@get('/dataset/:id/bundle')
def get_dataset_bundle():
    '''GET /dataset/:id/bundle'''
    pass



@get('/dataset/<id>/partitions')
def get_dataset_partitions_info(id):
    ''' GET    /dataset/:id/partitions''' 
    ds =  library.findByIdentity(id)
    if len(ds) == 0:
        return None
        
    if len(ds) > 1:
        raise Exception("Got more than one result")
    
    out = {}

    for partition in get_dataset_record(id).partitions:
        out[partition.id_] = partition.to_dict()
        
    return out;

@get('/partition/:pid/table/:tid/data')
def get_partition_data(pid,tid):
    pass

run(host='localhost', port=8080, reloader=True)
