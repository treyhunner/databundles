"""Client for CKAN data repositories


Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from databundles.run import RunConfig
import ckanclient

def get_client(rc=None, name=None):
    from databundles.dbexceptions import ConfigurationError
    if rc is None:
        rc = RunConfig()
        
    if name is None:
        name = 'default'
        
    try:
        catalog = rc.group('catalog')
        cfg = rc.catalog.get(name, False)
        url = cfg.url
        key = cfg.key
    except:
        raise ConfigurationError("Failed to get configuration for catalog.{0}.url or"+
                                 "catalog.{0}.key".format(name))
           
    return Ckan(url, key)
           
class Ckan(object):
    '''
    classdocs
    '''

    def __init__(self, url, key):
        '''
        Constructor
        '''
        import re

        if not re.search("/\d$", url): # prefer version 2 of the api
            url += '/2'

        # Instantiate the CKAN client.
        self.api = ckanclient.CkanClient(base_location=url,api_key=key)
        
    def get_or_new_group(self,name):
        from ckanclient import CkanApiNotFoundError
        
        try:
            group = self.api.group_entity_get(name)
        except CkanApiNotFoundError:
            group_entity = {
                'name': name,
                'title': name,
                'description': name
                }
            group = self.api.group_register_post(group_entity)
        
        return group
        
    def update_or_new_bundle(self, bundle):
    
        from ckanclient import CkanApiNotFoundError
        name = bundle.identity.name
            
        group = self.get_or_new_group('bundles')
   
        try:
            pe = self.api.package_entity_get(name)
           
        except CkanApiNotFoundError:
            # Register a minimal package, since we always will update. 
            package_entity = {'name': name}
            pe = self.api.package_register_post(package_entity)

        props = bundle.config.group('properties')

        package_entity = {
            'title':  props.get('title',None),
            'name': name,
            'author_email' : bundle.identity.creator,
            'author': props.get('author',None),
            'maintainer_email' : bundle.identity.creator,
            'maintainer': props.get('maintainer',None),            
            'extras': {
                'bundle/source' : bundle.identity.source,
                'bundle/dataset' : bundle.identity.dataset,
                'bundle/subset' : bundle.identity.subset,
                'bundle/variation' : bundle.identity.variation,
                'bundle/revision' : bundle.identity.revision,
                'bundle/id' : bundle.identity.id_
            },
                          
            'version':  bundle.identity.revision,
            'homepage':  props.get('homepage',None),
            'url':  props.get('url',None),
            'notes':  props.get('notes',None),
            'url':  props.get('url',None),
            'tags':  props.get('tags',None),
            'groups' : [group['id']]
        }
     
        pe = self.api.package_entity_put(package_entity)

        return pe
    
    def submit_bundle(self):
        pass
    
    def submit_partition(self, bunde_ref):
        pass
    
    def test_basic(self):

        uid = 'foobottom'
        
        package_list = ckan.package_register_get()
        print package_list

        package_entity = ckan.package_entity_get(uid)
     
        import pprint
        pprint.pprint(package_entity)

        import glob

        for f in glob.glob("/Volumes/DataLibrary/crime/months/*.csv"):
            parts = f.split('/').pop().strip('.csv').split('-')
            resource_tag = "{0}-{1:02d}".format(parts[0],int(parts[1]))
            print "Start ", resource_tag

            url,err = ckan.upload_file(f) #@UnusedVariable
            print "!!!",url

            r = ckan.add_package_resource(uid, f, 
                                          resource_type = 'data',
                                          description=resource_tag, 
                                          tags = resource_tag,
                                          author  = 1,
                                          creator = 2,
                                          dataset = 3, 
                                          subset = 4
                                          )
            
            pprint.pprint(r)