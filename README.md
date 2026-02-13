# nxcypher-bolt

Neo4j Bolt protocol server for nxcypher - enables standard Neo4j drivers to query NetworkX graphs.

> **⚠️ Licensing Notice**: This implementation is based on publicly documented Bolt protocol specifications.
> The Bolt protocol is developed by Neo4j, Inc. This project is not affiliated with or endorsed by Neo4j.
> Users should review Neo4j's terms of service and licensing before using this in production.

## Installation

```bash
pip install nxcypher-bolt
```

## Usage

### As a Library

```python
import asyncio
import networkx as nx
from nxcypher_bolt import BoltServer

# Create your graph
G = nx.DiGraph()
G.add_node("alice", name="Alice", age=30, __labels__={"Person"})
G.add_node("bob", name="Bob", age=25, __labels__={"Person"})
G.add_edge("alice", "bob", since=2020, __labels__={"KNOWS"})

# Start the Bolt server
server = BoltServer(G, host="0.0.0.0", port=7687)
asyncio.run(server.start())
```

### As a CLI

```bash
# Start server with a sample graph
nxcypher-bolt --host 0.0.0.0 --port 7687

# Or load a graph from a file
nxcypher-bolt --graph my_graph.gml
```

### Connecting with Neo4j Drivers

Once the server is running, connect using any Neo4j driver:

**Python:**
```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session() as session:
    result = session.run("MATCH (n:Person) RETURN n.name, n.age")
    for record in result:
        print(record["n.name"], record["n.age"])
```

**JavaScript:**
```javascript
const neo4j = require('neo4j-driver');

const driver = neo4j.driver('bolt://localhost:7687');
const session = driver.session();

const result = await session.run('MATCH (n:Person) RETURN n.name');
result.records.forEach(record => {
  console.log(record.get('n.name'));
});
```

## Supported Features

- **Bolt Protocol 4.4+**: Compatible with modern Neo4j drivers
- **Query Execution**: Full nxcypher query support via the Bolt protocol
- **Result Streaming**: Efficient PULL/DISCARD streaming
- **Transactions**: BEGIN/COMMIT/ROLLBACK support
- **Graph Types**: Works with any NetworkX graph (Graph, DiGraph, MultiGraph, MultiDiGraph)

## Architecture

```
Neo4j Driver (Python/JS/Java)
         │
         ▼ TCP port 7687
┌─────────────────────┐
│    BoltServer       │  ← asyncio TCP server
├─────────────────────┤
│  BoltConnection     │  ← per-connection handler
├─────────────────────┤
│  PackStream         │  ← binary serialization
│  Chunking           │  ← message framing
├─────────────────────┤
│  BoltSession        │  ← query/transaction state
│  ResultConverter    │  ← nxcypher → Bolt records
├─────────────────────┤
│    NXCypher         │  ← existing query engine
└─────────────────────┘
```

## License

Apache License 2.0

## Related Projects

- [nxcypher](https://github.com/your-org/nxcypher) - Cypher query engine for NetworkX
- [Neo4j](https://neo4j.com) - The original graph database with Bolt protocol
