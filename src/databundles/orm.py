'''
Created on Jun 21, 2012

@author: eric
'''
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
from sqlalchemy import Column as SAColumn, Integer
from sqlalchemy import Float as Real,  Text, ForeignKey
from sqlalchemy.orm import relationship, backref


class Dataset(Base):
    __tablename__ = 'datasets'

    oid = SAColumn('d_id',Text, primary_key=True)
    name = SAColumn('d_name',Integer, unique=True)
    source = SAColumn('d_source',Text)
    dataset = SAColumn('d_dataset',Text)
    subset = SAColumn('d_subset',Text)
    variation = SAColumn('d_variation',Text)
    creator = SAColumn('d_creator',Text)
    revision = SAColumn('d_revision',Text)

    tables = relationship("Table", backref='dataset', cascade='all', 
                          passive_updates=False)
    columns = relationship("Column", backref='dataset', cascade='all', 
                           passive_updates=False)

    def __init__(self,**kwargs):
        self.oid = kwargs.get("oid",None) 
        self.name = kwargs.get("name",None) 
        self.source = kwargs.get("source",None) 
        self.dataset = kwargs.get("dataset",None) 
        self.subset = kwargs.get("subset",None) 
        self.variation = kwargs.get("variation",None) 
        self.creator = kwargs.get("creator",None) 
        self.revision = kwargs.get("revision",None) 

        if not self.oid:
            from databundles.objectnumber import ObjectNumber
            self.oid = str(ObjectNumber())

    def init_id(self):
        '''Create a new dataset id'''
        

    def __repr__(self):
        return "<datasets: {}>".format(self.oid)
     

class Column(Base):
    __tablename__ = 'columns'

    sequence_id = SAColumn('c_id',Integer, primary_key=True)
    t_id = SAColumn('c_t_id',Text,ForeignKey('tables.t_id'))
    d_id = SAColumn('c_d_id',Text,ForeignKey('datasets.d_id'))
    name = SAColumn('c_name',Text, unique=True)
    altname = SAColumn('c_altname',Text)
    datatype = SAColumn('c_datatype',Text)
    size = SAColumn('c_size',Integer)
    precision = SAColumn('c_precision',Integer)
    flags = SAColumn('c_flags',Text)
    description = SAColumn('c_description',Text)
    keywords = SAColumn('c_keywords',Text)
    measure = SAColumn('c_measure',Text)
    units = SAColumn('c_units',Text)
    universe = SAColumn('c_universe',Text)
    scale = SAColumn('c_scale',Real)

    def __init__(self,**kwargs):
        self.sequence_id = kwargs.get("id",None) 
        self.t_id = kwargs.get("t_id",None) 
        self.d_id = kwargs.get("d_id",None) 
        self.name = kwargs.get("name",None) 
        self.altname = kwargs.get("altname",None) 
        self.datatype = kwargs.get("datatype",None) 
        self.size = kwargs.get("size",None) 
        self.precision = kwargs.get("precision",None) 
        self.flags = kwargs.get("flags",None) 
        self.description = kwargs.get("description",None) 
        self.keywords = kwargs.get("keywords",None) 
        self.measure = kwargs.get("measure",None) 
        self.units = kwargs.get("units",None) 
        self.universe = kwargs.get("universe",None) 
        self.scale = kwargs.get("scale",None) 

        # the table_name attribute is not stored. It is only for
        # building the schema, linking the columns to tables. 
        self.table_name = kwargs.get("table_name",None) 

    @property
    def oid(self):
        from databundles.objectnumber import ObjectNumber
        return ObjectNumber(self.d_id, int(self.t_id),int(self.sequence_id))

    def __repr__(self):
        try :
            return "<columns: {}>".format(self.oid)
        except:
            return "<columns: {}>".format(self.name)
 
   

class Table(Base):
    __tablename__ ='tables'

    sequence_id = SAColumn('t_id',Integer, primary_key=True)
    d_id = SAColumn('t_d_id',Text,ForeignKey('datasets.d_id'))
    name = SAColumn('t_name',Text, unique=True, nullable = False)
    altname = SAColumn('t_altname',Text)
    description = SAColumn('t_description',Text)
    keywords = SAColumn('t_keywords',Text)

    columns = relationship(Column, backref='table', cascade='all',
                            passive_updates=False)

    def __init__(self,**kwargs):
        self.sequence_id = kwargs.get("id",None) 
        self.d_id = kwargs.get("d_id",None) 
        self.name = kwargs.get("name",None) 
        self.altname = kwargs.get("altname",None) 
        self.description = kwargs.get("description",None) 
        self.keywords = kwargs.get("keywords",None) 

        if self.name:
            import re
            self.name = re.sub('[^\w_]','_', self.name).lower()

    @property
    def oid(self):
        from databundles.objectnumber import ObjectNumber
        return ObjectNumber(self.d_id, self.sequence_id)

    def add_column(self, name_or_column, **kwargs):

        import sqlalchemy.orm.session
        s = sqlalchemy.orm.session.Session.object_session(self)
        
        # Determine if the variable arg is a name or a column
        if isinstance(name_or_column, Column):
            kwargs = name_or_column.__dict__
            name = kwargs.get("name",None) 
        else:
            name = name_or_column
        
        try:
            row = (s.query(Column)
                   .filter(Column.t_id==self.name)
                   .filter(Column.d_id==self.d_id)
                   .filter(Column.t_id==self.sequence_id)
                   .one())
        except:
            row = Column(name=name, 
                        d_id=self.d_id,
                        t_id=self.sequence_id)
            s.add(row)
            
        for key, value in kwargs.items():
            if key[0] != '_' and key not in ['d_id','t_id','name','sequence_id']:
                setattr(row, key, value)
      
        s.commit()
        
        return row
        
    

    def __repr__(self):
        return "<tables: {}>".format(self.oid)
     

class Config(Base):
    __tablename__ = 'config'

    d_id = SAColumn('co_d_id',Text, primary_key=True)
    group = SAColumn('co_group',Text, primary_key=True)
    key = SAColumn('co_key',Text, primary_key=True)
    value = SAColumn('co_value',Text)
    source = SAColumn('co_source',Text)

    def __init__(self,**kwargs):
        self.d_id = kwargs.get("d_id",None) 
        self.group = kwargs.get("group",None) 
        self.key = kwargs.get("key",None) 
        self.value = kwargs.get("value",None)
        self.source = kwargs.get("source",None) 

    def __repr__(self):
        return "<config: {}>".format(self.oid)
     

class File(Base):
    __tablename__ = 'files'

    oid = SAColumn('f_id',Integer, primary_key=True, nullable=False)
    path = SAColumn('f_path',Text, nullable=False)
    process = SAColumn('f_process',Text)
    content_hash = SAColumn('f_hash',Text)
    modified = SAColumn('f_modified',Integer)
 
    def __init__(self,**kwargs):
        self.oid = kwargs.get("oid",None) 
        self.path = kwargs.get("path",None) 
        self.process = kwargs.get("process",None) 
        self.modified = kwargs.get("modified",None) 
        self.content_hash = kwargs.get("content_hash",None) 
      
     
    def __repr__(self):
        return "<files: {}>".format(self.path)

class BundlePartition(Base):
    __tablename__ = 'partitions'

    oid = SAColumn('p_id',Text, primary_key=True, nullable=False)
    t_id = SAColumn('p_t_id',Integer,ForeignKey('tables.t_id'))
    d_id = SAColumn('p_d_id',Text,ForeignKey('datasets.d_id'))
    space = SAColumn('p_space',Text)
    time = SAColumn('p_time',Text)
    name = SAColumn('p_name',Text)

    table = relationship("Table", backref='partitions', cascade='all', 
                         passive_updates=False)
    dataset = relationship("Dataset", backref='partitions', cascade='all', 
                           passive_updates=False)
    
    def __init__(self,**kwargs):
        self.id = kwargs.get("id",None) 
        self.t_id = kwargs.get("t_id",None) 
        self.d_id = kwargs.get("d_id",None) 
        self.space = kwargs.get("space",None) 
        self.time = kwargs.get("time",None) 
        self.name = kwargs.get("name",None) 

    def __repr__(self):
        return "<partitions: {}>".format(self.oid)
