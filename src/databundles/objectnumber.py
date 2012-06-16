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

    TCMAXVAL = 62*62 -1; # maximum for table and column values. 
    
    EPOCH = 1325376000 # Jan 1, 2012 in UNIX time

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

        self.table = table
        self.column = column

        # If the dataset is not defined, create a new dataset number. 
        if dataset == None:
            import time
            dataset = int(time.time())
        
        if isinstance(dataset, ObjectNumber):
            dataset = dataset.dataset
          
    
        if isinstance(dataset, int):
            
            # This calc is OK until 31 Dec 2053 00:00:00 GMT
            if dataset > self.EPOCH:
                dataset = dataset - self.EPOCH
                   
            self.dataset = ObjectNumber.base62_encode(dataset)
        elif isinstance(dataset, str):
            self.dataset = dataset
        
        else:
            raise TypeError

  
    def normalize_id(self):
        pass
  
    @classmethod
    def base62_encode(cls, num):
        """Encode a number in Base X
    
        `num`: The number to encode
        `alphabet`: The alphabet to use for encoding
        Stolen from: http://stackoverflow.com/a/1119769/1144479
        """
        
        alphabet="0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        
        if (num == 0):
            return alphabet[0]
        arr = []
        base = len(alphabet)
        while num:
            rem = num % base
            num = num // base
            arr.append(alphabet[rem])
        arr.reverse()
        return ''.join(arr)

    @classmethod
    def base62_decode(string):
        """Decode a Base X encoded string into the number
    
        Arguments:
        - `string`: The encoded string
        - `alphabet`: The alphabet to use for encoding
        Stolen from: http://stackoverflow.com/a/1119769/1144479
        """
        
        alphabet="0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        
        base = len(alphabet)
        strlen = len(string)
        num = 0
    
        idx = 0
        for char in string:
            power = (strlen - (idx + 1))
            num += alphabet.index(char) * (base ** power)
            idx += 1
    
        return num
       
    @property
    def object_type(self):
        return self.type
     
    
    @property
    def dataset_number(self):
        return ObjectNumber.base62_decode(self.dataset)
 
    @property 
    def table(self):
        return self.table_
    
    @table.setter
    def table(self, value): #@DuplicatedSignature         
        if isinstance(value, int):
            if value < 0 or value > self.TCMAXVAL:
                raise ValueError, "table argument must be between 0 and {0} ".format(self.TCMAXVAL)
            self.table_ = ObjectNumber.base62_encode(value).rjust(2,'0')
        elif isinstance(value, str):
            self.table_ = value
        elif value == None:
            self.table_ = None
        else:
            raise TypeError, "table argument must be an int or str. Got "+type(value).__name__

 
 
    @property
    def table_number(self):
        return ObjectNumber.base62_decode(self.table)

    
    @property
    def column_number(self):
        return ObjectNumber.base62_decode(self.column)
    
    
    @property 
    def column(self):
        return self.column_
    
    @column.setter
    def column(self, value): #@DuplicatedSignature
        if isinstance(value, int):              
            if value < 0 or value > self.TCMAXVAL:
                raise ValueError, "column argument must be between 0 and {0} ".format(self.TCMAXVAL)
            self.column_ = ObjectNumber.base62_encode(value).rjust(2,'0')
        elif isinstance(value, str):
            self.column_ = value
        elif value == None:
            self.column_ = None
        else:
            raise TypeError, "column argument must be an int or str. Got "+type(value).__name__
 
    
    
    def __str__(self):
        if self.column:
            return self._type+self.dataset+self.table+self.column
        elif self.table:
            return self._type+self.dataset+self.table
        else:
            return self._type+self.dataset
        
        
        