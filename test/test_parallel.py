'''
Created on Jul 12, 2012

@author: eric
'''
import unittest
import time

def func(t):
    print "Sleep for {}".format(t)
    time.sleep(t)
    return t

class Test(unittest.TestCase):


    def XtestProcessing(self):
        from processing import Pool #@UnresolvedImport
        pool = Pool(processes=4) 
        
        results = []
        for i in range(10):
            print 'Appending {}'.format(i)
            results.append(pool.apply_async(func, (2,)))
        
        for result in results:
            result.wait(1)

    def testMulti(self):
        from multiprocessing import Pool
        p = Pool(5)
        
        p.map(func, [ 2 for i in range(10)])


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()