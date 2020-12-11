![SQErzo logo](https://raw.githubusercontent.com/cr0hn/sqerzo/master/images/logo-250x250.png)

## `SQErzo` Tinty ORM for Graph databases

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [What is SQErzo](#what-is-sqerzo)
- [Which database are supported](#which-database-are-supported)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## What is SQErzo

`SQErzo` is simple and tiny ORM for graph databases. 

It's compatible with these databases that supports Open Cypher language.

## Which databases are supported

Currently, I did the test with these databases:

- Neo4j
- RedisGraph
- AWS Neptune (coming soon)
- Gremlin (coming soon)

## Usage examples

### Simple usage

Create some nodes and setup database in both databases:

- Neo4j
- RedisGraph

Without the need to change any code:

```python
from dataclasses import dataclass

from sqerzo import GraphEdge, GraphNode, SQErzoGraph as gh

class MeetEdge(GraphEdge):
    pass

@dataclass
class UserNode(GraphNode):
    name: str = None

    
def create_graph(connection_string: str):
    gh.setup(connection_string)
    gh.truncate()  # Drop database
    
    # Creates 20 relations
    for n in range(20):
        u1 = UserNode(name=f"UName-{n}")
        gh.get_or_create(u1)
        
        d1 = UserNode(name=f"DName-{n}")
        gh.get_or_create(d1)
        
        u1_meet_g1 = MeetEdge(
            source=u1,
            destination=d1
        )
        gh.get_or_create(u1_meet_g1)
        

if __name__ == '__main__':
    create_graph("redis://127.0.0.1:7000/?graph=email")   
    create_graph("neo4j://neo4j:s3cr3t@127.0.0.1:7687/?graph=email")   

```

This is the result database in Node4j:

![user_meet_neo4j logo](https://raw.githubusercontent.com/cr0hn/sqerzo/master/images/examples/user_meet_neo4j.png)

This is the result database in RedisGrap:

![user_meet_redisgraph logo](https://raw.githubusercontent.com/cr0hn/sqerzo/master/images/examples/user_meet_redisgraph.png)

### Load mail to a Graph

If you need a more complex example, you can find in it [examples/email_graph.py](https://github.com/cr0hn/sqerzo/blob/master/examples/email_graph.py).

At this example we load a random generated mail inbox (generation script is also available) into a Graph Database following this [Neo4j Blog Post](https://neo4j.com/blog/data-modeling-pitfalls/) suggestions.

![Fraud graph db](https://dist.neo4j.com/wp-content/uploads/20180730162521/corrected-fraud-detection-email-data-model-1024x994.png)

## TODO

- [ ] Finish the implementation for Gremlin based Graph databases
- [ ] Improve speed at insertion
- [ ] Add support for dates to RedisGraph using transformation of dates to numbers
- [ ] Add support for `UNIQUE` constraints
- [ ] Add support for `INDEXES` constraints
- [ ] Add support for raw Cypher query
- [ ] Errors, issues, new features and something else

## License

This project is distributed under [BSD license](https://github.com/cr0hn/sqerzo/blob/master/LICENSE>)
