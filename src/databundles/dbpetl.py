"""
"""

import petl

def follow(table, func, **kwargs):
    """
    Call a function for each non header row
    """   
 
    return FollowView(table, func, **kwargs)



class FollowView(petl.util.RowContainer):
    
    def __init__(self, source, f):
        self.source = source
        self.f = f
        
    def cachetag(self):
        return self.source.cachetag()
    
    def __iter__(self):
     
        for r in self.source:
            self.f(r)
            yield r
   
def mogrify(table, func, **kwargs):
    """
    Call a function for each row and replace the
    row with the one returned by the function
    """   
 
    return MogrifyView(table, func, **kwargs)



class MogrifyView(petl.util.RowContainer):
    
    def __init__(self, source, f):
        self.source = source
        self.f = f
        
    def cachetag(self):
        return self.source.cachetag()
    
    def __iter__(self):
        itr = iter(self.source)
        yield  itr.next() # Header
        for r in itr:
            yield self.f(r)
     
#
# Fluentize
#

import sys
from petl.fluent import FluentWrapper, wrap

#
# Add all of the functions in this module into the FluentWrapper as
# methods
#
for n, c in sys.modules[__name__].__dict__.items():
    if callable(c):
        setattr(FluentWrapper, n, wrap(c)) 
  