'''
Created on Jul 30, 2012

@author: eric
'''
import unittest
import itertools


class Test(unittest.TestCase):


    def test_m1(self):
        itr = range(0,10000000)
        n = 997 # prime
     
        sum  = 0 #@ReservedAssignment
        for i in xrange(0, len(itr), n):
            chunk = itr[i:i+n]
            sum += chunk[0]
  
    def x_test_m2(self):
        itr = range(0,10000000)
        n = 997 #prime
            
        sum = 0 #@ReservedAssignment
        args = [iter(itr)] * n
       
        for chunk in itertools.izip_longest(*args, fillvalue=None):
            sum += chunk[0]


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()