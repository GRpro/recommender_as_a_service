Recommender SaaS local environment
==================================

Cassandra
---------
Cassandra cluster (default 2 nodes) implementation with Docker Compose.

To start cluster use:
```bash
./start_cluster.sh
```
To stop cluster use:
```bash
./stop_cluster.sh
```

To remove cluster use:
```bash
./remove_cluster.sh
```
Note: Remove is possible only if it was already stopped.

To stop and remove cluster use:
```bash
./stop_cluster.sh && ./remove_cluster.sh
```

To scale cluster use the following script with desired number of nodes:
```bash
./scale_cluster.sh 4
```
If no number specified the default "1" node factor will be used. 
In this case Cassandra cluster with 2 nodes (seed and default node) will be started.
 
Redis
-----

Store job configurations