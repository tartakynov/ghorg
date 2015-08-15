from .tables import *
from contextlib import contextmanager

class DB:
    def __init__(self, engine):
        self._session = sessionmaker(bind = engine)
        Base.metadata.create_all(engine)

    @contextmanager
    def session(self, **kwargs):
        session = self._session(**kwargs)
        try:
            yield session
            if session.transaction:
                if session.transaction.is_active:
                    session.commit()
        except KeyboardInterrupt:
            if session.transaction:
                if session.transaction.is_active:
                    session.commit()
            raise
        except:
            if session.transaction:
                if session.transaction.is_active:
                    session.rollback()
            raise
        finally:
            session.flush()
            session.close()

def connect(url):
    engine = create_engine(url, echo = False)
    return DB(engine)
