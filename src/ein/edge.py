from typing import Dict


class Edge:
    """Edge object from the SQLite db."""

    def __init__(self, source_id: str, target_id: str, properties: Dict=None):
        """Representation of schema from {{schema_name}}_nodes."""
        self._source_id = source_id
        self._target_id = target_id
        self._properties = properties

    def get_source_id(self):
        return self._source_id

    def get_target_id(self):
        return self._target_id

    def get_properties(self):
        return self._properties

