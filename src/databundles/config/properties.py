'''
Created on Jun 14, 2012

@author: eric
'''


class SimpleProperty(object):
    """A simple property accessor
    In the calls below: self will be the SimpleProperty object, 
    and obj is the class of the object
    that holds the property
    """

    def __init__(self, name, default, doc=None):
        self.name = name
        self.value = default
        self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        #if obj is None:
        #    return self
    
        try:
            return obj.__dict__['_'+self.name]
        except KeyError:
            # We return none because the fact that we are in this function means
            # that the right property was in the class, even if it does 
            # not have a value set. The value was set in in this 
            # SimpleProperty instance, not in the target object. 
            return None
        

    def __set__(self, obj, value):        
        obj.__dict__['_'+self.name] = value

    def __delete__(self, obj):
        if self.fdel is None:
            raise AttributeError, "can't delete attribute"
        self.fdel(obj)
        
class DbRowProperty(object):
    """a property that is linked to a row in the database"""

    def __init__(self, name,  doc=None):
        self.name = name
        self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        return getattr(obj.row, self.name)

    def __set__(self, obj, value):   
        session = obj.bundle.database.session     
        setattr(obj.row, self.name, value)
        session.commit()
        session.refresh(obj.row)

    def __delete__(self, obj):
        raise AttributeError, "can't delete attribute"
  
        
class ConfigDbProperty(object):
    """"""

    def __init__(self, name,  doc=None):
        self.name = name
        self.__doc__ = doc

  

    def __get__(self, obj, objtype=None):

        from databundles.config.orm import Config

        db = obj.bundle.database 
        session = db.session

        group =  obj.__class__.__name__.lower()

        q = (session.query(Config)
             .filter(Config.group== group)
             .filter(Config.key == self.name))

        try:
            config = q.one()
        except:
            config = Config(group=group, key=self.name)
            session.add(config)
            session.commit()
            
            
        try:
            return config.value
        except KeyError:
            # We return none because the fact that we are in this function means
            # that the right property was in the class, even if it does not 
            # have a value set. The value was set in in this SimpleProperty 
            # instance, not in the target object. 
            return None
        

    def __set__(self, obj, value):        
        obj.__dict__['_'+self.name] = value

    def __delete__(self, obj):
        if self.fdel is None:
            raise AttributeError, "can't delete attribute"
        self.fdel(obj)