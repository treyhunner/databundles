'''
Created on Jun 10, 2012

@author: eric
'''

from sqlite import SqliteDatabase

class ProtoDB(SqliteDatabase):
    '''
    classdocs
    '''

    def __init__(self, params):
        '''
        Constructor
        '''
        