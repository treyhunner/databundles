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
        
        if isinstance(dataset, ObjectNumber):
            # Copy constructor
            if table is None: 
                table = dataset.table
                
            if column is None:
                column = dataset.column
            
            dataset = dataset.dataset
                 
        elif isinstance(dataset, int):
            
            # This calc is OK until 31 Dec 2053 00:00:00 GMT
            if dataset > self.EPOCH:
                dataset = dataset - self.EPOCH
          
        elif isinstance(dataset, str) or isinstance(dataset, unicode):
            
            if  isinstance(dataset, unicode):
                dataset = dataset.encode('ascii')
          
            if dataset[0] == self.TYPE.DATASET:
                dataset = int(ObjectNumber.base62_decode(dataset[1:]))
            elif dataset[0] == self.TYPE.TABLE:
                if table is None:
                    table = int(ObjectNumber.base62_decode(dataset[-2:]))
                dataset = int(ObjectNumber.base62_decode(dataset[1:-2]))
            elif dataset[0] == self.TYPE.COLUMN:       
                if column is None:
                    column = int(ObjectNumber.base62_decode(dataset[-2:]))
                if table is None:
                    table = int(ObjectNumber.base62_decode(dataset[-4:-2]))
                dataset = int(ObjectNumber.base62_decode(dataset[1:-4]))
            else:
                raise ValueError('Unknow type character: '+dataset[0])
           
            
        elif dataset is None:
            import time
            dataset = int(time.time())-self.EPOCH
        else:
            raise TypeError('dataset value must be an integer or a string. got: '
                            +str(type(dataset)))

        if table is None and column is None:
            self.type = self.TYPE.DATASET
            
        elif table is not None and column is None:
            self.type = self.TYPE.TABLE
            
        elif table is not None and column is not None:  
            self.type = self.TYPE.COLUMN
            
        else:
            raise "Bad Arguments";

        if table is not None:
            self._table = table
        else:
            self._table = None
            
        if column is not None:
            self._column = column
        else:
            self._column = None

        if isinstance(dataset, str):
            self.dataset =ObjectNumber.base62_decode(dataset)
        elif isinstance(dataset, int):
            self.dataset = dataset
        else:
            raise ValueError('Data set must end up as a string or an int')
        
        if self._table >  self.TCMAXVAL:
            raise ValueError, "table argument must be between 0 and {0} ".format(self.TCMAXVAL)
        
        if self._column >  self.TCMAXVAL:
            raise ValueError, "column argument must be between 0 and {0} ".format(self.TCMAXVAL)
        
       
 
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
    def base62_decode(cls,string):
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
        
        if self._table >  self.TCMAXVAL:
            raise ValueError, "table argument must be between 0 and {0} ".format(self.TCMAXVAL)
        
        return self._table
    
    @table.setter
    def table(self, value): #@DuplicatedSignature         
        if isinstance(value, int):
            if value < 0 or value > self.TCMAXVAL:
                raise ValueError, "table argument must be between 0 and {0} ".format(self.TCMAXVAL)
            self._table = value 
        elif isinstance(value, str) or isinstance(value, unicode) :
            self._table = ObjectNumber.base62_decode(value)
        elif value is None:
            self._table = None
        else:
            raise TypeError, "table argument must be an int or str. Got "+type(value).__name__
   
    @property 
    def column(self):
        
        if self._column >  self.TCMAXVAL:
            raise ValueError, "column argument must be between 0 and {0} ".format(self.TCMAXVAL)
        
        return self._column
    
    @column.setter
    def column(self, value): #@DuplicatedSignature
        if isinstance(value, int):              
            if value < 0 or value > self.TCMAXVAL:
                raise ValueError, "column argument must be between 0 and {0} ".format(self.TCMAXVAL)
            self._column = value
        elif isinstance(value, str):
            self._column =  ObjectNumber.base62_decode(value)
        elif value is None:
            self._column = None
        else:
            raise TypeError, "column argument must be an int or str. Got "+type(value).__name__
 
    
    
    def __str__(self):
        if self.type == self.TYPE.COLUMN:
  
            return (self.type+
                    ObjectNumber.base62_encode(self.dataset)+
                    ObjectNumber.base62_encode(self.table).rjust(2,'0')+
                    ObjectNumber.base62_encode(self.column).rjust(2,'0'))
            
        elif self.type == self.TYPE.TABLE:
            return (self.type+
                    ObjectNumber.base62_encode(self.dataset)+
                    ObjectNumber.base62_encode(self.table).rjust(2,'0'))
        else:
            return (self.type+ObjectNumber.base62_encode(self.dataset))
           
    def __repr__(self):
        return "<ObjectNumber:{}:{}:{}:{}>".format(self.type, self.dataset, self._table, self._column)

   
        