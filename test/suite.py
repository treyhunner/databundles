'''
Created on Oct 15, 2012

@author: eric
'''

import unittest
import test_bundle
import test_library
import test_server

if __name__ == '__main__':
    suite = unittest.TestSuite()
    
    suite.addTests(test_bundle.suite())
    suite.addTests(test_library.suite())
    suite.addTests(test_server.suite())

    unittest.TextTestRunner().run(suite)