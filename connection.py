from typing import List, Any, Sequence, Tuple

from streamlit.connections import ExperimentalBaseConnection
from streamlit.runtime.caching import cache_data
from streamlit.errors import StreamlitAPIException
from qdrant_client import QdrantClient
from qdrant_client.conversions import common_types as types


class QdrantStreamlitConnection(ExperimentalBaseConnection[QdrantClient]):

    def _connect(self, **kwargs) -> QdrantClient:
        if 'host' in kwargs and 'port' in kwargs:
            if 'url' in kwargs:
                raise StreamlitAPIException(
                    'You should specify either host and port or the url, not both.')
            db = f"{kwargs.pop('host')}:{kwargs.pop('port')}"

        if 'url' in kwargs and 'api_key' in self._secrets:
            return QdrantClient(url=kwargs.pop('url'), api_key=self._secrets['api_key'], **kwargs)
        else:
            db = kwars.pop('url')
        return QdrantClient(url=db, **kwargs)

    @property
    def cursor(self) -> QdrantClient:
        return self._instance

    def close(self, **kwargs):
        self._instance.close()

    def search_vector(self, collection_name: str, query_vector: Union[
        types.NumpyArray,
        Sequence[float],
        Tuple[str, List[float]],
        types.NamedVector,
    ], limit: int = 10, ttl: int = 300, **kwargs: Any) -> List[types.ScoredPoint]:

        @cache_data(ttl=ttl)
        def _search_vector(collection_name: str, query_vector: Union[
            types.NumpyArray,
            Sequence[float],
            Tuple[str, List[float]],
            types.NamedVector,
        ], limit: int = 10, **kwargs: Any):
            return self._instance.search(collection_name, query_vector, limit=limit)

        return _search_vector()
