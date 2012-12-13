'''
Created on Jun 22, 2012

@author: eric
'''
import unittest
from  testbundle.bundle import Bundle
from databundles.identity import * #@UnusedWildImport
import time, logging
import databundles.util

logger = databundles.util.get_logger(__name__)


class TestBase(unittest.TestCase):

    def copy_or_build_bundle(self):
        """Set up a clean bundle build, either by re-building the bundle, or
        by copying it from a saved bundle directory """
        
        # For most cases, re-set the bundle by copying from a saved version. If
        # the bundle doesn't exist and the saved version doesn't exist, 
        # build a new one. 

        bundle = Bundle()  
        marker = bundle.filesystem.build_path('test-marker')
        build_dir =  bundle.filesystem.build_path()+'/' # Slash needed for rsync
        save_dir = bundle.filesystem.build_path()+"-save/"

        if not os.path.exists(marker):
            logger.info( "Build dir marker ({}) is missing".format(marker))
            # There is a good reason to create a seperate instance, 
            # but don't remember what it is ... 
            
            bundle.clean()
            
            bundle = Bundle()   
            if not os.path.exists(save_dir):
                logger.info( "Save dir is missing; re-build bundle. ")
                bundle.prepare()
                bundle.build()
                
                with open(marker, 'w') as f:
                    f.write(str(time.time()))
                # Copy the newly built bundle to the save directory    
                os.system("rm -rf {1}; rsync -arv {0} {1} > /dev/null ".format(build_dir, save_dir))

        # Always copy, just to be safe. 
        logger.info(  "Copying bundle from {}".format(save_dir))
        os.system("rm -rf {0}; rsync -arv {1} {0}  > /dev/null ".format(build_dir, save_dir))
 