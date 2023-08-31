from streamlit.connections import ExperimentalBaseConnection
from qdrant_client import QdrantClient


class QdrantStreamlitConnection(ExperimentalBaseConnection[QdrantClient]):

    def _connect(self, **kwargs) -> QdrantClient:
        if 'database' in kwargs:
            db = kwargs.pop('database')
        else:
            db = self._secrets['database']
        return QdrantClient(url=db, **kwargs)
