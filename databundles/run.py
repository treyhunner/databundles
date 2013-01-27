"""Runtime configuration logic for running a bundle build. 

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import os.path
from databundles.util import AttrDict


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
        path -- If present, a yaml file to load last, overwriting earlier values.
          If it is an array, load only the files in the array. 
          
        If path is not an array, the file is added to an array of files, and
        each of the files is loaded. These files are: 
        
            /etc/databundles.yaml
            ~/.databundles.yaml
            databundles.yaml
        '''
        
        self.config = AttrDict()
        self.config['loaded'] = []
    
        if isinstance(path, list):
            self.files = path
        else:
            self.files = ['/etc/databundles.yaml', 
                     os.path.expanduser('~/.databundles.yaml'), 
                     os.path.join(os.getcwd(),'databundles.yaml'),
                     path ]

        loaded = False
        for f in self.files:
            if f is not None and os.path.exists(f):
                loaded = True
                self.config.loaded.append(f)
                self.config.update_yaml(f)

        if not loaded:
            raise Exception("Failed to load any config from: {}".format(self.files))

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
        
        result = unittest.TextTestResult(sys.stdout, True, 1) #@UnusedVariable
        
        print "Loading tests from ",dir
        for test in tests:
            print "Running ", test
            test.context.bundle = b
            unittest.TextTestRunner().run(test)

                
    
    