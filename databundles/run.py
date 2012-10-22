'''
Created on Jun 10, 2012

@author: eric
'''

import yaml
import os.path

import itertools as it, operator as op, functools as ft
from collections import Mapping, OrderedDict, defaultdict
import os, sys

try: import yaml, yaml.constructor
except ImportError: pass

def patch_file_open():
    import __builtin__
    openfiles = set()
    oldfile = __builtin__.file
    class newfile(oldfile):
        def __init__(self, *args):
            self.x = args[0]
            print "### {} OPENING {} ###".format(len(openfiles), str(self.x))         
            oldfile.__init__(self, *args)
            openfiles.add(self)
    
        def close(self):
            print "### {} CLOSING {} ###".format(len(openfiles), str(self.x))
            oldfile.close(self)
            openfiles.remove(self)
            
    oldopen = __builtin__.open
    
    def newopen(*args):
        return newfile(*args)
    
    __builtin__.file = newfile
    __builtin__.open = newopen

#patch_file_open()

# From http://pypi.python.org/pypi/layered-yaml-attrdict-config/12.07.1
class OrderedDictYAMLLoader(yaml.Loader):
    'Based on: https://gist.github.com/844388'

    def __init__(self, *args, **kwargs):
        yaml.Loader.__init__(self, *args, **kwargs)
        self.add_constructor(u'tag:yaml.org,2002:map', type(self).construct_yaml_map)
        self.add_constructor(u'tag:yaml.org,2002:omap', type(self).construct_yaml_map)

    def construct_yaml_map(self, node):
        data = OrderedDict()
        yield data
        value = self.construct_mapping(node)
        data.update(value)

    def construct_mapping(self, node, deep=False):
        if isinstance(node, yaml.MappingNode):
            self.flatten_mapping(node)
        else:
            raise yaml.constructor.ConstructorError( None, None,
                'expected a mapping node, but found {}'.format(node.id), node.start_mark )

        mapping = OrderedDict()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError, exc:
                raise yaml.constructor.ConstructorError( 'while constructing a mapping',
                    node.start_mark, 'found unacceptable key ({})'.format(exc), key_node.start_mark )
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping

# http://pypi.python.org/pypi/layered-yaml-attrdict-config/12.07.1
class AttrDict(OrderedDict):

    def __init__(self, *argz, **kwz):
        super(AttrDict, self).__init__(*argz, **kwz)

    def __setitem__(self, k, v):
        super(AttrDict, self).__setitem__( k,
            AttrDict(v) if isinstance(v, Mapping) else v )
    def __getattr__(self, k):
        if not (k.startswith('__') or k.startswith('_OrderedDict__')): 
            return self[k]
        else: 
            return super(AttrDict, self).__getattr__(k)
    def __setattr__(self, k, v):
        if k.startswith('_OrderedDict__'):
            return super(AttrDict, self).__setattr__(k, v)
        self[k] = v

    @classmethod
    def from_yaml(cls, path, if_exists=False):
        if if_exists and not os.path.exists(path): return cls()
        return cls(yaml.load(open(path), OrderedDictYAMLLoader))

    @staticmethod
    def flatten_dict(data, path=tuple()):
        dst = list()
        for k,v in data.iteritems():
            k = path + (k,)
            if isinstance(v, Mapping):
                for v in v.flatten(k): dst.append(v)
            else: dst.append((k, v))
        return dst

    def flatten(self, path=tuple()):
        return self.flatten_dict(self, path=path)

    def update_flat(self, val):
        if isinstance(val, AttrDict): val = val.flatten()
        for k,v in val:
            dst = self
            for slug in k[:-1]:
                if dst.get(slug) is None:
                    dst[slug] = AttrDict()
                dst = dst[slug]
            if v is not None or not isinstance(
                dst.get(k[-1]), Mapping ): dst[k[-1]] = v

    def update_dict(self, data):
        self.update_flat(self.flatten_dict(data))

    def update_yaml(self, path): 
        self.update_flat(self.from_yaml(path))

    def clone(self):
        clone = AttrDict()
        clone.update_dict(self)
        return clone

    def rebase(self, base):
        base = base.clone()
        base.update_dict(self)
        self.clear()
        self.update_dict(base)

    def dump(self, stream):
        yaml.representer.SafeRepresenter.add_representer(
            AttrDict, yaml.representer.SafeRepresenter.represent_dict )
        yaml.representer.SafeRepresenter.add_representer(
            OrderedDict, yaml.representer.SafeRepresenter.represent_dict )
        yaml.representer.SafeRepresenter.add_representer(
            defaultdict, yaml.representer.SafeRepresenter.represent_dict )
        yaml.representer.SafeRepresenter.add_representer(
            set, yaml.representer.SafeRepresenter.represent_list )
        yaml.safe_dump( self, stream,
            default_flow_style=False, indent=4, encoding='utf-8' )



def configure_logging(cfg, custom_level=None):
    import logging, logging.config
    if custom_level is None: custom_level = logging.WARNING
    for entity in it.chain.from_iterable(it.imap(
            op.methodcaller('viewvalues'),
            [cfg] + list(cfg.get(k, dict()) for k in ['handlers', 'loggers']) )):
        if isinstance(entity, Mapping)\
            and entity.get('level') == 'custom': entity['level'] = custom_level
    logging.config.dictConfig(cfg)
    logging.captureWarnings(cfg.warnings)




class RunConfig(object):
    '''
    Runtime configuration. 
    
    The RunConfig object will search for a databundles.yaml file in multiple locations, 
    including: 
    
      /etc/databundles.yaml
      ~user/.databundles.yaml
      The current directory
      A named path ( --config option )
      
    It will start from the first directory, and for each one, try to load the file
    and copy the values into an accumulator, with later values overwritting
    earlier ones. 
    '''

    def __init__(self, path=None):
        '''Create a new RunConfig object
        
        Arguments
        path -- If present, a yaml file to load last, overwriting earlier values
        '''
        
        
        self.config = AttrDict()
        self.config['loaded'] = []
    
        self.files = ['/etc/databundles.yaml', 
                 os.path.expanduser('~/.databundles.yaml'), 
                 os.path.join(os.getcwd(),'databundles.yaml'),
                 path ]
    
        for f in self.files:
            if f is not None and os.path.exists(f):
                self.config.loaded.append(f)
                self.config.update_yaml(f)

    def __getattr__(self, group):
        '''Fetch a confiration group and return the contents as an 
        attribute-accessible dict'''
        return self.config.get(group,{})

    def group(self, name):
        '''return a dict for a group of configuration items.'''
        
        return self.config.get(name,{})

    def dump(self, stream=None):
        
        to_string = False
        if stream is None:
            import StringIO
            stream = StringIO.StringIO()
            to_string = True
            
        self.config.dump(stream)
        
        if to_string:
            stream.seek(0)
            return stream.read()
        else:
            return stream
        
        


def run(argv, bundle_class):
   
  
    b = bundle_class()
    args = b.parse_args(argv)
   
    if args.test:
        print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        print "!!!!!! In Test Mode !!!!!!!!!!"
        print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        import time
        time.sleep(1)

    if 'clean' in args.phases:
        b.clean()
        
    if 'prepare' in args.phases:
        if b.pre_prepare():
            b.log("---- Preparing ----")
            if b.prepare():
                b.post_prepare()
                b.log("---- Done Preparing ----")
            else:
                b.log("---- Prepare exited with failure ----")
                return False
        else:
            b.log("---- Skipping prepare ---- ")
    else:
        b.log("---- Skipping prepare ---- ") 
        
    if 'build' in args.phases:
        
        if b.run_args.test:
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            print "!!!!!! In Test Mode !!!!!!!!!!"
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

            time.sleep(1)
            
        if b.pre_build():
            b.log("---- Build ---")
            if b.build():
                b.post_build()
                b.log("---- Done Building ---")
            else:
                b.log("---- Build exited with failure ---")
                return False
        else:
            b.log("---- Skipping Build ---- ")
    else:
        b.log("---- Skipping Build ---- ") 
    
    if 'install' in args.phases:
        if b.pre_install():
            b.log("---- Install ---")
            if b.install():
                b.post_install()
                b.log("---- Done Installing ---")
            else:
                b.log("---- Install exited with failure ---")
        else:
            b.log("---- Skipping Install ---- ")
    else:
        b.log("---- Skipping Install ---- ")      
     
    if 'submit' in args.phases:
        if b.pre_submit():
            b.log("---- Submit ---")
            if b.submit():
                b.post_submit()
                b.log("---- Done Submitting ---")
            else:
                b.log("---- Submit exited with failure ---")
        else:
            b.log("---- Skipping Submit ---- ")
    else:
        b.log("---- Skipping Submit ---- ")            
                
    if 'test' in args.phases:
        ''' Run the unit tests'''
        import nose, unittest, sys

        dir = b.filesystem.path('test') #@ReservedAssignment
                         
                   
        loader = nose.loader.TestLoader()
        tests =loader.loadTestsFromDir(dir)
        
        result = unittest.TextTestResult(sys.stdout, True, 1)
        
        print "Loading tests from ",dir
        for test in tests:
            print "Running ", test
            test.context.bundle = b
            unittest.TextTestRunner().run(test)

                
    
    