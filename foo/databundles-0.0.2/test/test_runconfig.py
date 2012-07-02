'''
Created on Jun 30, 2012

@author: eric
'''
import unittest

from databundles.runconfig import RunConfig

class Test(unittest.TestCase):


    def test_basic(self):
        rc = RunConfig()
        
        rc.overlay({'repository':{
                                  'one':1,
                                  'two':2
                                  }
                    })
        
        rc.overlay({'repository':{
                                
                                  'two':2.2,
                                  'three':3
                                  }
                    })
        
        import yaml
        
        print rc.group('library').get('root')
        
        print yaml.dump(rc.config)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_basic']
    unittest.main()