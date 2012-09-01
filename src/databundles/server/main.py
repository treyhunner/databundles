'''
REST Server For DataBundle Libraries. 
'''

from bottle import  run, get, put, post, request #@UnresolvedImport

import databundles.library 
import databundles.run

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

@get('/dataset/<id_>')    
def get_dataset_info( id_):
    '''Return a single dataset given an id_ or name'''
    
    return get_dataset_record(id_).identity.to_dict()

@get('/dataset/:id/bundle')
def get_dataset_bundle():
    '''GET /dataset/:id/bundle'''
    pass



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

@put('/test')
def put_test():
    print "TEST PUT"
    
    if request.content_type == 'application/json':
            v = request.json
            return {str(type(v)): v}
    else:
            v = request.body.getvalue() #@UndefinedVariable
            return {str(type(v)): v}
    
    print request.content_type


run(host='localhost', port=8080, reloader=True)
