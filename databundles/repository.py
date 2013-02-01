"""Interface to the CKAN data repository, for uploading bundle records and data extracts. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
from databundles.dbexceptions import ConfigurationError

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

        
   
    def eval_for_expr(self, astr,debug=False):
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

        
    def _do_extract(self, package,  extract_data):
        """Extract a CSV file and  upload it to CKAN"""
        import tempfile
        import uuid
        import os
        import petl.fluent as petlf

        p = extract_data['partition']
     
        
        f  = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()) )
    
        self.bundle.log("Extracting and submitting: {} as {}".format(p.identity.name, f))
        self.bundle.log("Extract query: {}".format(extract_data['query']))
            
        petlf.fromsqlite3(p.database.path, extract_data['query'] ).tocsv(f) #@UndefinedVariable

        re = self.api.add_file_resource(package, f, name= extract_data['name'],  
                                  description=extract_data['description'],
                                 content_type = "text/csv", format='csv')
        
        self.bundle.log("Extracted: {}: {}".format(extract_data['name'], extract_data['title']))
        
        os.remove(f)
        
        return re
        
    def _make_ge_dict(self, p, extract, each):
        '''Return a dict that includes the fields from the extract expanded for
        the values of each and the partition'''
        
        qd = {
              'p_id' : p.identity.id_,
              'p_name' : p.identity.name,
              'p_table' : p.identity.table,
              'p_space' : p.identity.space,
              'p_time' : p.identity.time,
              'p_grain' : p.identity.grain,              
              }
        
        for k,v in each.items():
            qd[k] = v
        
        r = qd
        
        r['partition'] = p
        r['query'] = extract.get('query','').format(**qd)
        r['title'] = extract.get('title','').format(**qd)
        r['description'] = extract.get('description','').format(**qd)
        r['name'] = extract.get('name','').format(**qd)
        
        return r
    
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
            each = [each[0]]
        

        # Now the top level arrays of each are dimensions, and we can do a 
        # multi dimensional iteration over them. 
        # This is essentially a cross-product, where out <- out X dim(i)

        import pprint
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
            partitions = self.partitions
        else:
            partition = self.partitions.get(partition_name)
            partitions = [partition]
            
        out = []
         
        if not for_:
            for_ = 'True'
         
        for partition in partitions:
            try:
                self.eval_for_expr(for_, True)
                if eval(for_):  
                    out.append(partition)
            except Exception as e:
                self.bundle.error("Error in evaluting for '{}' : {} ".format(for_, e))
          
        return out
            
    def generate_extracts(self,):
        """Generate dicts that have the data for an extract, along with the 
        partition, query, title and description """
        ext_config = self.extracts

        for extract in ext_config:
            for_ = extract.get('for', False)
            each = extract.get('each', [])
            p_id = extract.get('partition', False)
            if not p_id:
                continue

            partitions = self._expand_partitions(p_id, for_)
            datasets= self._expand_each(each)

            for partition in partitions:
                for data in datasets:     
                    qe = self._make_ge_dict(partition, extract, data)
        
                    yield qe
                    
    def submit(self): 

        ckb = self.api.update_or_new_bundle_extract(self.bundle)
        
        # Clear out existing resources. 
        ckb['resources'] = []      
        self.api.put_package(ckb)
        
        for extract_data in self.generate_extracts():
            self._do_extract(ckb,  extract_data)
        
        return True