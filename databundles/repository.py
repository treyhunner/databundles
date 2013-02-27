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
        
        repo_group = self.bundle.config.group('repository')
        if not repo_group.get(repo_name): 
            raise ConfigurationError("'repository' group in configure either nonexistent"+
                                     " or missing {} sub-group ".format(repo_name))

        repo_config = repo_group.get(repo_name)
        
        self.api = databundles.client.ckan.Ckan( repo_config.url, repo_config.key)   

        
   
    def _validate_for_expr(self, astr,debug=False):
        """Check that an expression is save to evaluate"""
        import ast
        try: tree=ast.parse(astr)
        except SyntaxError: raise ValueError(astr)
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
                                ast.Eq
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
            raise ValueError("Bad node {} in {}".format(node,astr))
        return True

    def _do_extract(self, extract_data):

        done_if = extract_data.get('done_if',False)
        if done_if and self._validate_for_expr(done_if, True):
            if eval(done_if):  
                return True

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
    
        file_ = f(extract_data,file_name=extract_data.get('name', f_name))        

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
        #self.bundle.log("  Dict: {}".format(extract_data))  
          
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
         
    def _sub(self, dict):
        dict['query'] = dict.get('query','').format(**dict)
        dict['title'] = dict.get('title','').format(**dict)
        dict['description'] = dict.get('description','').format(**dict)
        dict['name'] = dict.get('name','').format(**dict)
        
        return dict

 
            
    def generate_extracts(self):
        """Generate dicts that have the data for an extract, along with the 
        partition, query, title and description """
        import collections
        from databundles.util import toposort
        
        ext_config = self.extracts

        # Order the extracts to satisfy dependencies. 
        graph = {}
        for key,extract in ext_config.items():
            graph[key] = set(extract.get('depends',[]))
     
        exec_list = []
        for group in toposort(graph):
            exec_list.extend(group)
            

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
          
    def extract(self):
        import os
        
        for extract_data in self.generate_extracts():
            file_ = self._do_extract(extract_data)
            if file_ and os.path.exists(file_):
                self.bundle.log("Extracted: {}".format(file_))
            else:
                self.bundle.error("Extracted file {} does not exist".format(file_))
       
        return True
                    
    def submit(self): 
        """Create a dataset for the bundle, then add a resource for each of the
        extracts listed in the bundle.yaml file"""
        
        self.bundle.update_configuration()
        from os.path import  basename
    
        ckb = self.api.update_or_new_bundle_extract(self.bundle)
        
        sent = set()
        
        # Clear out existing resources. 
        ckb['resources'] = []      
        self.api.put_package(ckb)
        
        for doc in self.bundle.config.group('about').get('documents',[]):
            self.store_document(ckb, doc)

        for extract_data in self.generate_extracts():

            file_ = self._do_extract(extract_data)
            if file_ not in sent:
                r = self._send(ckb, extract_data,file_)
                sent.add(file_)
                url = r['ckan_url']
                self.bundle.log("Submitted {} to {}".format(basename(file_), url))
            else:
                self.bundle.log("Already processed {}, not sending.".format(basename(file_)))
        
        return True