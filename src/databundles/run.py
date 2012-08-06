'''
Created on Jun 10, 2012

@author: eric
'''
import sys, getopt

def get_args(argv):
    import argparse
    
    parser = argparse.ArgumentParser(prog='python bundle.py',
                                     description='Run the bunble build process')
    
    parser.add_argument('phases', metavar='N', type=str, nargs='*',
                   help='Build phases to run')
    
    parser.add_argument('-r','--reset',  default=False, action="store_true",  
                        help='')
    
    parser.add_argument('-b','--build_opt', action='append', help='Set options for the build phase')
    

    
    args = parser.parse_args()
    
    if args.build_opt is None:
        args.build_opt = []
        
    return args
    

import yaml
import os.path

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
        
        self.config = {}
        self.config['loaded'] = []
    
        self.load('/etc/databundles.yaml')
        self.load(os.path.expanduser('~/.databundles.yaml'))
        self.load(os.path.join(os.getcwd(),'databundles.yaml'))
        
        if path is not None:
            self.load(os.path.join(path))
        
    @property
    def dict(self): #@ReservedAssignment
        return self.config
    
    def overlay(self,config):
        '''Overlay the values from an input dictionary into 
        the object configuration, overwritting earlier values. '''
        
        for name,group in config.items():
            
            if not name in self.config:
                self.config[name] = {}
            
            try:
                for key,value in group.items():
                    self.config[name][key] = value
            except:
                # item is not a group
                self.config[name] = group 
    
    def load(self,path):

        if os.path.exists(path):
            
            try:
                d = yaml.load(file(path, 'r'))
            except yaml.scanner.ScannerError as e:
                from exceptions import ConfigurationError
                raise ConfigurationError("Error in YAML configuration file {} : {}"
                                         .format(path, str(e)))
            self.overlay(d)
            self.config['loaded'].append(path)
        

    def __getattr__(self, group):
        '''Fetch a confiration group and return the contents as an 
        attribute-accessible dict'''
        
        inner = self.dict[group]
        
        class attrdict(object):
            def __setattr__(self, key, value):
                key = key.strip('_')
                inner[key] = value

            def __getattr__(self, key):
                key = key.strip('_')
                if key not in inner:
                    return None
                
                return inner[key]
        
        return attrdict()

    def group(self, name):
        '''return a dict for a group of configuration items.'''
        
        return self.config.get(name,{})



def run(argv, bundle_class):
   
    args = get_args(argv) #@UnusedVariable

    if len(args.phases) ==  0 or (len(args.phases) == 1 and args.phases[0] == 'all'):    
        phases = ['prepare','build', 'install']
    else:
        phases = args.phases

    b = bundle_class()
    b.run_args = args


    if 'clean' in phases:
        b.clean()
        
    if 'prepare' in phases:
        if b.pre_prepare():
            b.log("---- Preparing ----")
            if b.prepare():
                b.post_prepare()
                b.log("---- Done Preparing ----")
            else:
                b.log("---- Prepare exited with failure ----")
        else:
            b.log("---- Skipping prepare ---- ")
    else:
        b.log("---- Skipping prepare ---- ") 
        
    if 'build' in phases:
        if b.pre_build():
            b.log("---- Build ---")
            if b.build():
                b.post_build()
                b.log("---- Done Building ---")
            else:
                b.log("---- Build exited with failure ---")
        else:
            b.log("---- Skipping Build ---- ")
    else:
        b.log("---- Skipping Build ---- ") 
    
    if 'install' in phases:
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
     
    if 'submit' in phases:
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
                
    if 'test' in phases:
        import nose

        dir = b.filesystem.path('test') #@ReservedAssignment
                   
        loader = nose.loader.TestLoader()
        
        tests = loader.loadTestsFromDir(dir)
        
        print tests
        
        
                
    
    