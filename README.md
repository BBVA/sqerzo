![SQErzo logo](https://raw.githubusercontent.com/cr0hn/sqerzo/master/images/logo-250x250.png)

## `SQErzo` Tinty ORM for Graph databases

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [What is SQErzo](#what-is-sqerzo)
- [Which databases are supported](#which-databases-are-supported)
- [Why use SQErzo?](#why-use-sqerzo)
- [Project status](#project-status)
- [Install](#install)
- [Usage examples](#usage-examples)
  - [Run databases uses Docker.](#run-databases-uses-docker)
    - [Start Neo4j](#start-neo4j)
    - [Start RedisGraph](#start-redisgraph)
  - [Simple usage](#simple-usage)
  - [Load mail to a Graph](#load-mail-to-a-graph)
- [TODO](#todo)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## What is SQErzo

`SQErzo` is simple and tiny ORM for graph databases. 

It's compatible with these databases that supports Open Cypher language.

## Which databases are supported

| Database | Status |
| --- | --- |
| Neo4j | Supported |
| Redis Graph | Supported |
| Arango DB | Looking for contributor |
| AWS Neptune| Looking for contributor |
| Gremlin | Looking for contributor |

## Why use SQErzo?

`SQErzo` intermediates between the Graph DB and your code and can manage database differences between them. For examples:

- RedisGraph doesn't support Date times or CONSTRAINTS, `SQErzo` does the magic to hide that.
- Neo4j need different channels for writing than for read. `SQErzo` does the magic to hide that.
- `SQErzo` integrates a in memory cache to avoid queries to Graph DB and try to improve the performance.
- Every database uses their own Node/Edge identification system. You need to manage and understand then to realize when a node already exits in Graph DB. `SQErzo` do this for you. It doesn't matter the Graph DB engine you use.
- `SQErzo` was made to avoid you to write useless code. You can create and manage Nodes and Edges in a few lines of code without know Graph DB internals.
- `SQErzo` supports Graph DB bases on Open cypher language (a Graph databases query language). You don't need to learn them to perform day a day operations. 

## Project status

Project is in a very early stage. If you want to use them, have in count that. 

## Install

Install is easy. Only run:

```shell
> pip install sqerzo
```

## Usage examples

### Run databases uses Docker.

#### Start Neo4j

```shell
> docker run -d -p7474:7474 -p7687:7687 -e NEO4J_AUTH=neo4j/s3cr3t neo4j
```

#### Start RedisGraph

```shell
> docker run -p 7000:6379 -d --rm redislabs/redisgraph
```

### Simple usage

Create some nodes and setup database in both databases:

- Neo4j
- RedisGraph

Without the need to change any code:

```python
from dataclasses import dataclass

from sqerzo import GraphEdge, GraphNode, SQErzoGraph

class MeetEdge(GraphEdge):
    pass

@dataclass
class UserNode(GraphNode):
    name: str = None

    
def create_graph(connection_string: str):
    gh = SQErzoGraph(connection_string)
    gh.truncate()  # Drop database
    
    u1 = UserNode(name=f"UName-1")
    gh.save(u1)
    
    d1 = UserNode(name=f"DName-2")
    gh.save(d1)
        
    u1_meet_g1 = MeetEdge(
        source=u1,
        destination=d1
    )
    gh.save(u1_meet_g1)
        

if __name__ == '__main__':
    create_graph("redis://127.0.0.1:7000/?graph=email")   
    create_graph("neo4j://neo4j:s3cr3t@127.0.0.1:7687/?graph=email")
```

This is the result database in Node4j:

![user_meet_neo4j logo](https://raw.githubusercontent.com/cr0hn/sqerzo/master/images/examples/user_meet_neo4j.png)

This is the result database in RedisGrap:

![user_meet_redisgraph logo](https://raw.githubusercontent.com/cr0hn/sqerzo/master/images/examples/user_meet_redisgraph.png)

### Transactions

Transactions are useful if you need add a lot of data. You add nodes and edges to a transaction. When they finish then perform the insertions to the database in a very efficient way:

```python
from dataclasses import dataclass

from sqerzo import GraphEdge, GraphNode, SQErzoGraph

class MeetEdge(GraphEdge):
    pass

@dataclass
class UserNode(GraphNode):
    __keys__ = ["name"]

    name: str = None

def create_graph(connection_string: str):
    gh = SQErzoGraph(connection_string)
    gh.truncate()  # Drop database

    with gh.transaction() as tx:  # Transaction starts here

        for n in range(500):  # Inserts 1000 nodes (500 * 2) and 500 relations
            u1 = UserNode(name=f"UName-{n}")
            d1 = UserNode(name=f"DName-{n}")

            tx.add(u1)
            tx.add(d1)

            u1_meet_g1 = MeetEdge(
                source=u1,
                destination=d1
            )
            tx.add(u1_meet_g1)


if __name__ == '__main__':
    print("Redis...")
    create_graph("redis://127.0.0.1:7000/?graph=email")
    print("Neo4j...")
    create_graph("neo4j://neo4j:s3cr3t@127.0.0.1:7687/?graph=email")
```

## Examples

### Load mails to a Graph

If you need a more complex example, you can find in it [examples/email_graph.py](https://github.com/cr0hn/sqerzo/blob/master/examples/email_graph.py).

At this example we load a random generated mail inbox (generation script is also available) into a Graph Database following this [Neo4j Blog Post](https://neo4j.com/blog/data-modeling-pitfalls/) suggestions.

![Fraud graph db](https://dist.neo4j.com/wp-content/uploads/20180730162521/corrected-fraud-detection-email-data-model-1024x994.png)

## ChangeLog

### Release 0.1.0

- [X] Improved speed at insertion by 100x
- [X] Add support for `UNIQUE` create_constraints_nodes
- [X] Add support for `INDEXES` create_constraints_nodes
- [X] Add support for raw Cypher query
- [x] Errors, issues, new features and something else
- [x] Complete refactor to easy add new backends
- [x] Complete refactor to easy add new backends
- [x] Add new methods: fetch_many, fetch_one, raw_query, save, update & transaction
- [x] Add new examples
- [x] Improved the way to build the Node to avoid waste memory.

## TODO

- [ ] Improve documentation
- [ ] Improve cypher query to avoid query raises when a transaction insert a duplicate node
- [ ] Add support for Arango DB
- [ ] Add support for AWS Neptune
- [ ] Add support for Gremlin
- [ ] Add support for dates to RedisGraph using transformation of dates to numbers

## References

I tried to use good practices for building `SQErzo`. Some references I used:

- https://medium.com/neo4j/cypher-query-optimisations-fe0539ce2e5c
- https://hub.packtpub.com/advanced-cypher-tricks/
- https://gist.github.com/jexp/caeb53acfe8a649fecade4417fb8876a

## License

This project is distributed under [BSD license](https://github.com/cr0hn/sqerzo/blob/master/LICENSE>)
