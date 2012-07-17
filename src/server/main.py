'''

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

@get('/dataset/<id>')    
@get('/dataset/<id>/info') 
def get_dataset_info( id):
    '''Return a single dataset given an id or name'''
    
    ds =  library.findByIdentity(id)
    
    if len(ds) == 0:
        return None
        
    if len(ds) > 1:
        raise Exception("Got more than one result")
    
    return ds.pop().identity.to_dict()
  

@get('/dataset/:id/bundle')
def get_dataset_bundle():
    '''GET    /dataset/:id/bundle'''
    pass
    
@get('/dataset/<id>/partitions/info')
def get_dataset_partitions_info():
    ''' GET    /dataset/:id/partitions/info''' 
    pass
    

run(host='localhost', port=8080, reloader=True)
