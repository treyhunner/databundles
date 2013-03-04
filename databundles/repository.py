"""Interface to the CKAN data repository, for uploading bundle records and data extracts. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
from databundles.dbexceptions import ConfigurationError
import petl.fluent as petlf

class Repository(object):
    '''Interface to the CKAN data repository, for uploading bundle records and 
    data extracts. classdocs
    '''

    def __init__(self, bundle, repo_name='default'):
        '''Create a new repository interface
        '''  
        import databundles.client.ckan
        import time, datetime

        self.bundle = bundle 
        self.extracts = self.bundle.config.group('extracts')
        self.partitions = self.bundle.partitions   
        self.repo_name = repo_name
        self._api = None
   
    @property
    def api(self):
        if not self._api:
            self.set_api()
            
        return self._api

    def set_api(self): 
        import databundles.client.ckan
        repo_group = self.bundle.config.group('repository')
        
        if not repo_group.get(self.repo_name): 
            raise ConfigurationError("'repository' group in configure either nonexistent"+
                                     " or missing {} sub-group ".format(self.repo_name))
        
        repo_config = repo_group.get(self.repo_name)
        
        self._api =  databundles.client.ckan.Ckan( repo_config.url, repo_config.key)   
        
        return self.api
        
   
    def _validate_for_expr(self, astr,debug=False):
        """Check that an expression is save to evaluate"""
        import os
        import ast
        try: tree=ast.parse(astr)
        except SyntaxError: raise ValueError(
                    "Could not parse code expression : \"{}\" ".format(astr)+
                    " ")
        for node in ast.walk(tree):
            if isinstance(node,(ast.Module,
                                ast.Expr,
                                ast.Dict,
                                ast.Str,
                                ast.Attribute,
                                ast.Num,
                                ast.Name,
                                ast.Load,
                                ast.BinOp,
                                ast.Compare,
                                ast.Eq,
                                ast.Import,
                                ast.alias,
                                ast.Call
                                )): 
                continue
            if (isinstance(node,ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == 'datetime'): 
                continue
            if debug:
                attrs=[attr for attr in dir(node) if not attr.startswith('__')]
                print(node)
                for attrname in attrs:
                    print('    {k} ==> {v}'.format(k=attrname,v=getattr(node,attrname)))
            raise ValueError("Bad node {} in {}. This code is not allowed to execute".format(node,astr))
        return True

    def _do_extract(self, extract_data, force=False):
        import os # For the eval @UnusedImport
        
        done_if = extract_data.get('done_if',False)
 
        if not force and done_if and self._validate_for_expr(done_if, True):
            if eval(done_if): 
                self.bundle.log("For extract {}, done_if ( {} ) evaluated true"
                         .format(extract_data['_name'], done_if)) 
                return extract_data['path']

        if extract_data.get('function',False):
            file_ = self._do_function_extract(extract_data)
        elif extract_data.get('query',False):
            file_ = self._do_query_extract(extract_data)
        else:
            from databundles.dbexceptions import ConfigurationError
            raise ConfigurationError("Bad Extract config: {}".format(extract_data))

        return file_
        
    def _do_function_extract(self, extract_data):
        '''Run a function on the build that produces a file to upload'''
        import os.path
        
        f_name = extract_data['function']
        
        f = getattr(self.bundle, f_name)
    
        file_ = f(extract_data)        

        return file_

                       
    def _do_query_extract(self,  extract_data):
        """Extract a CSV file and  upload it to CKAN"""
        import tempfile
        import uuid
        import os

        p = extract_data['_partition'] # Set in _make_partition_dict
     
        file_name = extract_data.get('name', None)
        
        if file_name:
            file_ = self.bundle.filesystem.path('extracts', file_name)
        else:
            file_ =  os.path.join(tempfile.gettempdir(), str(uuid.uuid4()) )

    
        self.bundle.log("Extracting {} to {}".format(extract_data['title'],file_))

        petlf.fromsqlite3(p.database.path, extract_data['query'] ).tocsv(file_) #@UndefinedVariable
  
        return file_       
    
    def _send(self, package, extract_data, file_):
        import os
        import mimetypes
        
        _, ext = os.path.splitext(file_)
        mimetypes.init()
        content_type = mimetypes.types_map.get(ext,None)  #@UndefinedVariable
        
        try:
            _,format = content_type.split('/')
        except:
            format = None
        
        name = extract_data.get('name', os.path.basename(file_))
        
        
        r = self.api.add_file_resource(package, file_, 
                            name=name,
                            description=extract_data['description'],
                            content_type = content_type, 
                            format=format
                            )
        
        
        return r
        
    def _make_partition_dict(self, p):
        '''Return a dict that includes the fields from the extract expanded for
        the values of each and the partition'''
        
        qd = {
            'p_id' : p.identity.id_,
            'p_name' : p.identity.name,
         }
        
        try:
            # Bundles don't have these      
            qd_part = {
                'p_table' : p.identity.table,
                'p_space' : p.identity.space,
                'p_time' : p.identity.time,
                'p_grain' : p.identity.grain,              
                }
        except:
            qd_part = {'p_table' : '','p_space' : '', 'p_time' :'','p_grain' : ''}
            
        qd =  dict(qd.items()+ qd_part.items())
        qd['_partition'] = p

        return qd
    
    def _expand_each(self, each):
        '''Generate a set of dicts from the cross product of each of the
        arrays of 'each' group'''
        
        # Normalize the each group, particular for the case where there is only
        # one dimension
  
        if not isinstance(each, list):
            raise ConfigurationError("The 'each' key must have a list. Got a {} ".format(type(each)))
        
        elif len(each) == 0:
            each = [[{}]]
        if not isinstance(each[0], list):
            each = [each]
        

        # Now the top level arrays of each are dimensions, and we can do a 
        # multi dimensional iteration over them. 
        # This is essentially a cross-product, where out <- out X dim(i)

        out = []
        for i,dim in enumerate(each):
            if i == 0:
                out = dim
            else:
                o2 = []
                for i in dim:
                    for j in out:
                        o2.append(dict(i.items()+j.items()))
                out = o2

        return out
        

        
    def _expand_partitions(self, partition_name='any', for_=None):
        '''Generate a list of partitions to apply the extract process to. '''

        if partition_name == 'any':
            partitions = [p for p in self.partitions]
            partitions = [self.bundle] + partitions
        else:
            partition = self.partitions.get(partition_name)
            partitions = [partition]

        out = []
         
        if not for_:
            for_ = 'True'
         
        for partition in partitions:
         
            try:
                self.bundle.log("Testing: {} ".format(partition.identity.name))
                if self._validate_for_expr(for_, True):
                    if eval(for_):  
                        out.append(partition)
            except Exception as e:
                self.bundle.error("Error in evaluting for '{}' : {} ".format(for_, e))
          
        return out
         
    def _sub(self, data):
        
        if data.get('aa', False):
            from databundles.geo.analysisarea import get_analysis_area

            aa = get_analysis_area(self.bundle.library, **data['aa'])    
        
            aa_d  = dict(aa.__dict__)
            aa_d['aa_name'] = aa_d['name']
            del  aa_d['name']
            
            data = dict(data.items() + aa_d.items())

        data['query'] = data.get('query','').format(**data)
        data['title'] = data.get('title','').format(**data)
        data['description'] = data.get('description','').format(**data)
        data['name'] = data.get('name','').format(**data)
        data['path'] = self.bundle.filesystem.path('extracts',format(data['name']))
        data['done_if'] = data.get('done_if','').format(**data)
  
        return data

    
    def dep_tree(self, root):
        """Return the tree of dependencies rooted in the given nod name, 
        excluding all other nodes"""
        
        graph = {}
        for key,extract in self.extracts.items():
            graph[key] = set(extract.get('depends',[]))
            
        def _recurse(node):
            l = set([node])
            for n in graph[node]:
                l = l | _recurse(n)
            
            return l
            
        return  _recurse(root)
            
            
    def generate_extracts(self, root=None):
        """Generate dicts that have the data for an extract, along with the 
        partition, query, title and description
        
        :param root: The name of an extract group to use as the root of
        the dependency tree
        :type root: string
        
        If `root` is specified, it is a name of an extract group from the configuration,
        and the only extracts performed will be the named extracts and any of its
        dependencies. 
    
         """
        import collections
        from databundles.util import toposort
        
        ext_config = self.extracts

        # Order the extracts to satisfy dependencies. 
        graph = {}
        for key,extract in ext_config.items():
            graph[key] = set(extract.get('depends',[]))
     

        if graph:
            exec_list = []
            for group in toposort(graph):
                exec_list.extend(group)
        else:
            exec_list = ext_config.keys()
            
        if root:
            deps = self.dep_tree(root)
            exec_list = [ n for n in exec_list if n in deps]
         
       
        # now can iterate over the list. 
        for key in exec_list:
            extract = ext_config[key]
            extract['_name'] = key
            for_ = extract.get('for', "'True'")
            function = extract.get('function', False)
            each = extract.get('each', [])
            p_id = extract.get('partition', False)
            eaches = self._expand_each(each)
  
            # This part is a awful hack and should be refactored
            if function:
                for data in eaches:  
                    yield self._sub(dict(extract.items() + data.items()))

            elif p_id:       
                partitions = self._expand_partitions(p_id, for_)
    
                for partition in partitions:
                    p_dict = self._make_partition_dict(partition)
                    for data in eaches:     
                        yield self._sub(dict(p_dict.items()+extract.items() + 
                                             data.items() ))
              
    def store_document(self, package, config):
        import re, string

        id =  re.sub('[\W_]+', '-',config['title'])
        
        r = self.api.add_url_resource(package, 
                                        config['url'], 
                                        config['title'],
                                        description=config['description'])
        
        return r
          
    def extract(self, root=None, force=False):
        import os

        for extract_data in self.generate_extracts(root=root):
            file_ = self._do_extract(extract_data, force=force)
            if file_ is True:
                #self.bundle.log("Extract {} marked as done".format(extract_data['_name']))
                pass
            elif file_ and os.path.exists(file_):
                self.bundle.log("Extracted: {}".format(file_))
            else:
                self.bundle.error("Extracted file {} does not exist".format(file_))
       
        return True
                    
    def submit(self,  root=None, force=False, repo=None): 
        """Create a dataset for the bundle, then add a resource for each of the
        extracts listed in the bundle.yaml file"""
        
        if repo:
            self.repo_name = repo
            self.set_api()
        
        self.bundle.update_configuration()
        from os.path import  basename
    
        ckb = self.api.update_or_new_bundle_extract(self.bundle)
        
        sent = set()
        
        # Clear out existing resources. 
        ckb['resources'] = []      
        self.api.put_package(ckb)
        
        for doc in self.bundle.config.group('about').get('documents',[]):
            self.store_document(ckb, doc)

        for extract_data in self.generate_extracts(root=root):

            file_ = self._do_extract(extract_data, force=force)
            if file_ not in sent:
                r = self._send(ckb, extract_data,file_)
                sent.add(file_)
                url = r['ckan_url']
                self.bundle.log("Submitted {} to {}".format(basename(file_), url))
            else:
                self.bundle.log("Already processed {}, not sending.".format(basename(file_)))
        
        return True