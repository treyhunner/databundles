'''
Created on Jun 14, 2012

@author: eric
'''


class SimpleProperty(object):
    """A simple property accessor
    In the calls below: self will be the SimpleProperty object, and obj is the class of the object
    that holds the property
    """

    def __init__(self, default, doc=None):
        self.value = default
        self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
    
        return self.value

    def __set__(self, obj, value):
        self.value = value

    def __delete__(self, obj):
        if self.fdel is None:
            raise AttributeError, "can't delete attribute"
        self.fdel(obj)