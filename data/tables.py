from sqlalchemy import create_engine
from sqlalchemy import Column, BigInteger, Integer, String, Boolean, DateTime, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Organization(Base):
    __tablename__ = 'organizations'
    id = Column(Integer, primary_key = True)
    name = Column(String(255), nullable = False)
    location = Column(String(255))
    public_repos = Column(Integer)
    forks_2014 = Column(Integer)
    url_github = Column(String(255))
    url_site = Column(String(255))
    created_at = Column(DateTime)

    uix_name = UniqueConstraint(name)

    def __repr__(self):
        return "<Organization(id=%d, name='%s')>" % (self.id, self.name)

class NotFound(Base):
    __tablename__ = 'notfound'
    name = Column(String(255), primary_key = True)
