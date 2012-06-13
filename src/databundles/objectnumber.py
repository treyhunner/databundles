'''
Created on Jun 11, 2012

@author: eric
'''

class ObjectNumber(object):
    '''
    classdocs
    '''
    class _const:
        class ConstError(TypeError): pass
        def __setattr__(self,name,value):
            if self.__dict__.has_key(name):
                raise self.ConstError, "Can't rebind const(%s)"%name
            self.__dict__[name]=value

    TYPE=_const()
    TYPE.DATASET = 'a'
    TYPE.TABLE ='b'
    TYPE.COLUMN = 'c'

    def __init__(self, dataset=None, table = None, column=None):
        '''
        Constructor
        '''
        
        if table == None and column == None:
            self._type = self.TYPE.DATASET
        elif table != None and column == None:
            self._type = self.TYPE.DATASET
        elif table != None and column != None:  
            self._type = self.TYPE.DATASET
        else:
            raise "Bad Arguments";
               
        
        @property
        def object_type():
            return self.type
        
        
        
        