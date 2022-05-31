from typing import Dict


class Node:
    """Node object from the SQLite db."""

    def __init__(self,
                 schema_name: str,
                 id: str,
                 body: Dict) -> None:
        """Representation of 'node' from {{schema_name}}_nodes."""
        self.schema_name = schema_name
        self.id = id
        self.body = body

    def __repr__(self) -> str:
        pass

