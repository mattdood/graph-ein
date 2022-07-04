<div align="center">
    <img src="https://github.com/mattdood/graph-ein/raw/master/assets/ein-space.gif" alt="Gif of Ein from Cowboy Bebop in space"/>
</div>

**Note:** I do not make any claims to the [Cowboy Bebop](https://en.wikipedia.org/wiki/Cowboy_Bebop) assets, names, or trademarks.

# Graph-Ein
A graph database implemented in [SQLite](https://sqlite.org/index.html). This is
great for creating a standalone, single file graph database.

<img src="https://img.shields.io/github/issues/mattdood/graph-ein"
    target="https://github.com/mattdood/graph-ein/issues"
    alt="Badge for GitHub issues."/>
<img src="https://img.shields.io/github/forks/mattdood/graph-ein"
    target="https://github.com/mattdood/graph-ein/forks"
    alt="Badge for GitHub forks."/>
<img src="https://img.shields.io/github/stars/mattdood/graph-ein"
    alt="Badge for GitHub stars."/>
<img src="https://img.shields.io/github/license/mattdood/graph-ein"
    target="https://github.com/mattdood/graph-ein/raw/master/LICENSE"
    alt="Badge for GitHub license, MIT."/>
<img src="https://img.shields.io/twitter/url?url=https%3A%2F%2Fgithub.com%2Fmattdood%2Fgraph-ein"
    target="https://twitter.com/intent/tweet?text=Wow:&url=https%3A%2F%2Fgithub.com%2Fmattdood%2Fgraph-ein"
    alt="Badge for sharable Twitter link."/>
[![Pytest](https://github.com/mattdood/graph-ein/actions/workflows/ci.yml/badge.svg)](https://github.com/mattdood/graph-ein/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/ein-graph.svg)](https://badge.fury.io/py/ein-graph)

## Installation
To install the project, run the following:

```
pip install ein-graph
```

## Usage
The project generates a database file if one is not provided. The database itself
is accessible; however, the `Graph` object is the typical interface that should
be used for interacting with the data stored.

### Database
SQLite database files are created for each separate database path that is provided.
These are utilized upon instantiation of the `Graph` object, which will discover
each of the data objects stored in the file.

The database can be connected to via [DBeaver](https://dbeaver.io) or some other
database client if you'd like to explore the data.

**Note:** For users that want to utilize additional SQLite features there are
methods for executing arbitrary statements/multiple statements.

#### Schemas
Databases have a concept of "schemas" that are used to organize disparate nodes
and edges from each other. As such, a schema is needed for each of the node/edge
data points added.

Schemas are stored in table-form as follows:
```
<schema_name>_nodes
<schema_name>_edges
```

To look at these in action use the following on your database using a database client:
```sql
-- from "select-schemas.sql"
SELECT
    name
FROM
    sqlite_master
WHERE
    type = 'table'
    AND
    tbl_name LIKE '%' || ? || '%'
;
```
**Note:** The above should only be used after a setup has been performed to create
a client and add a schema.

### Graph
The `Graph` instantiates with all current data in the supplied path.

Graphs operate with "schemas" that help to logically separate different sub-graphs.
These graphs can be connected via `Edge` objects to bridge clusters of nodes.

Any data added/updated/deleted via the `Graph` is also reflected at the database
level at time of execution.

```python
from ein.graph import Graph


graph = Graph(db_path="test.db")
graph.add_schema("some_schema")
```

### Nodes
Each `Node` object is a representation of data stored in the database, all data
should be capable of being rendered to JSON.

```python
from ein.node import Node


node = Node(
    schema_name="some_schema",
    id="my-id",
    body={
        "some-key": 1,
        "other-key": "data"
    },
)
```

### Edges
The graph's `Edge`s are a pair of nodes with a relationship. These contain the
`source` and `target` nodes (and optionally `properties` for weights).

```python
from ein.edge import Edge


edge = Edge(
    schema_name="some_schema",
    source=Node(...),
    target=Node(...),
)
```

To bridge `Node` objects between two schemas the `schema_name=` argument
should be supplied. This will be the database schema that the edge is stored in,
but the schemas of the `source` and `target` will be discovered from the `Node`
objects.

```python
from ein.edge import Edge


node_one = Node(
    schema_name="some_schema",
    ...,
)

node_two = Node(
    schema_name="some_other_schema",
    ...,
)

edge = Edge(
    schema_name=node_one.schema_name,
    source=node_one,
    target=node_two,
)

print(edge.source_schema_name) # some_schema
print(edge.target_schema_name) # some_other_schema
```

## Releasing builds
To release builds for the project we use a combination of tagging and changes to
`setup.py`.

For any releases to `test.pypi.org` use a change to the `version="..."` inside of
`setup.py`. Once a PR is merged into the main project the test release will be updated.

Any prod releases to `pypi.org` require a tagged version number. This should be done locally
by running the following:

```bash
git checkout master
git pull master
git tag v<version-number-here>
git push origin v<version-number-here>
```

### Rollbacks of versions
To roll a version back we need to delete the tagged release from the prod PyPI,
then delete the GitHub tag.

```
git tag -d v<version-number-here>
git push origin :v<version-number-here>
```

