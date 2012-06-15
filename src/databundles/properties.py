'''
Created on Jun 14, 2012

@author: eric
'''


class SimpleProperty(object):
    """A simple property accessor
    In the calls below: self will be the SimpleProperty object, and obj is the class of the object
    that holds the property
    """

    def __init__(self, name, default, doc=None):
        self.name = name
        self.value = default
        self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        #if obj is None:
        #    return self
    
        #print 'get nam: '+str(self.name)
        #print 'get self: '+str(self);
        #print 'get obj: '+str(obj);
    
        try:
            return obj.__dict__['_'+self.name]
        except KeyError:
            # We return none because the fact that we are in this function means
            # that the right property was in the class, even if it does not have a value set. 
            # The value was set in in this SImpleProperty instance, not in the target object. 
            return None
        

    def __set__(self, obj, value):
        #print 'set nam: '+str(self.name)
        #print 'set slf: '+str(self)
        #print 'set obj: '+str(obj)
        #print 'set val: '+str(value)
        
        obj.__dict__['_'+self.name] = value

    def __delete__(self, obj):
        if self.fdel is None:
            raise AttributeError, "can't delete attribute"
        self.fdel(obj)