'''
Created on Aug 1, 2012

@author: eric
'''

def coerce_int(v):   
    '''Convert to an int, or return if isn't an int'''
    try:
        return int(v)
    except:
        return v
        
                
class PassthroughTransform(object):
    '''
    Pasthorugh the value unaltered
    '''

    def __init__(self, column, useIndex=False):
        """
        """
                # Extract the value from a position in the row
        if useIndex:
            f = lambda row, column=column: row[column.sequence_id-1]
        else:
            f = lambda row, column=column: row[column.name]

        self.f = f

        
    def __call__(self, row):
        return self.f(row)
       
                
class BasicTransform(object):
    '''
    A Callable class that will take a row and return a value, cleaned according to
    the classes cleaning rules. 
    '''

    @staticmethod
    def basic_defaults(v, column, default, f):
        '''Basic defaults method, using only the column default and illegal_value
        parameters. WIll also convert blanks and None to the default '''
        if v is None:
            return default
        elif v == '':
            return default
        elif str(v) == column.illegal_value:
            return default
        else:
            return f(v)

    def __init__(self, column, index=None):
        """
        
        """
        self.column = column
  
        # for numbers try to coerce to an integer. We'd have to use a helper func
        # with a try/catch, except in this case, integers are always all digits here 
        if column.datatype == 'integer':
            f = lambda v: int(v)
        else:
            f = lambda v: v

        if column.default is not None:
            if column.datatype == 'text':
                default = column.default 
            else:
                default = int(column.default)
        else:
            default = None
        
        if default:
            f = (lambda v, column=column, f=f, default=default, defaults_f=self.basic_defaults : 
                    defaults_f(v, column, default, f) )

        # Strip test values, but not numbers
        f = lambda v, f=f:  f(v.strip()) if isinstance(v,basestring) else f(v)
        
        # Extract the value from a position in the row
        f = lambda row, column=column, f=f: f(row[column.name])
        
        self.f = f

        
    def __call__(self, row):
        return self.f(row)
       
       
class CensusTransform(BasicTransform):
    '''
    Transformation that condsiders the special codes that the Census data may
    have in integer fields. 
    ''' 
    
    @staticmethod
    def census_defaults(v, column, default, f):
        '''Basic defaults method, using only the column default and illegal_value
        parameters. WIll also convert blanks and None to the default '''
        if v is None:
            return default
        elif v == '':
            return default
        elif str(v) == ('9' * column.width):
            return default
        elif v.startswith('!'):
            return -2
        elif v.startswith('#'):
            return -3
        else:
            return f(v)
    
    def __init__(self, column, useIndex=False):
        """
        A Transform that is designed for the US Census, converting codes that
        apear in Integer fields. The geography data dictionary in 
        
            http://www.census.gov/prod/cen2000/doc/sf1.pdf
            
        
        Assignment of codes of nine (9) indicates a balance record or that 
        the entity or attribute does not exist for this record.

        Assignment of pound signs (#) indicates that more than one value exists for 
        this field and, thus, no specific value can be assigned.

        Assignment of exclamation marks (!) indicates that this value has not yet 
        been determined or this file.
        
        This transform makes these conversions: 
        
            The Column's illegal_value becomes -1
            '!' becomes -2
            #* becomes -3

        Args:
            column an orm.Column
            useIndex if True, acess the column value in the row by index, not name
            

        """
        self.column = column
  
        # for numbers try to coerce to an integer. We'd have to use a helper func
        # with a try/catch, except in this case, integers are always all digits here 
        if column.datatype == 'integer':
            f = lambda v: int(v)
        else:
            f = lambda v: v

        if column.default and column.default.strip():
            if column.datatype == 'text':
                default = column.default 
            else:
                default = int(column.default)
        else:
            default = None
        
        
        f = (lambda v, column=column, f=f, default=default, defaults_f=self.census_defaults : 
                    defaults_f(v, column, default, f) )

        # Strip test values, but not numbers
        f = lambda v, f=f:  f(v.strip()) if isinstance(v,basestring) else f(v)

        # Extract the value from a position in the row
        if useIndex:
            f = lambda row, column=column, f=f: f(row[column.sequence_id-1])
        else:
            f = lambda row, column=column, f=f: f(row[column.name])

        
        self.f = f
    
    
    
    
    
    