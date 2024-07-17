# SquirrelDB HA setup

This setup provide both scalability and availability of a Prometheus remote store using SquirrelDB.

To achieve this goal, you will need:

* A cluster of 3 or more Cassandra node
* A Redis cluster (start at 6 nodes)
* A load-balancer in front of SquirrelDB (e.g. nginx)
* two or more of SquirrelDB instances

For the details on how to configure a Cassandra cluster or Redis cluster, look at their
respective documentation or follow the quickstart below.

SquirrelDB instance will be stateless (they are stateless, because short term data will be stored in Redis) and
need the following configurations:

```
cassandra:
  # You should have enough addresses here to have at least one that respond.
  # Once connected, SquirrelDB will get all other Cassandra node from the one it connected to.
  # So either have a majority a node addresses or a virtual address always reachable (load-balancer, Kubernetes service, ...)
  addresses:
  - "cassandra1:9042"
  - "cassandra2:9042"
  # Replication factor should be odd, since SquirrelDB rely on quorum for it's consistency
  replication_factor: 3
redis:
  # Like for Cassandra addresses, you just need to have enough addresses to connected to one
  # working Redis node, from here client will discover all nodes in the cluster.
  addresses: "redis1:6379,redis2:6379"
```

## Quickstart

This quickstart will start a SquirrelDB in HA (with Cassandra cluster and Redis) using docker compose.
Obvisouly this is only for testing since all components will run on the same machine.

Due to some component not liking to change their IP address, they are fixed and use
subnet 172.28.0.0/16. If this conflict with one of your existing network, update
the docker compose file to change this subnet.

Like the single-node quickstart, this will also start a Prometheus + Grafana and one node_exporter to
have some data in SquirrelDB.

Start the stack:

```
docker compose up -d
```

Then as for single SquirrelDB, go to http://localhost:3000 (default credentials are admin/admin) and:

* Add a Prometheus datasource (using http://squirreldb:9201 as URL)
* Create your dashboard or import a dashboard (for example import dashboard with ID 1860).

### Testing availability

To test availability, you can stop+start or remove/recreate component.

Example of test:

```
# Recreate Prometheus, losing its local store of metrics. This ensure that metric points
# are read from SquirrelDB
docker compose up -d --force-recreate --renew-anon-volumes prometheus

# Kill one Cassandra then restart it
docker compose stop -t0 cassandra1
docker compose start cassandra1

# Stop and recreate one SquirrelDB
docker compose rm --force --stop -v squirreldb2
docker compose up -d squirreldb2

docker compose stop -t0 redis4
docker compose up -d redis4
docker compose stop -t0 redis2
docker compose up -d redis2
```

For Cassandra, if you remove and recreate a Cassandra (e.g. lose data and not just stop/start):

* You should do this with cassandra3, or remove the node from CASSANDRA_SEEDS (only required on the new node itself, other may
  be untouched). Failling to do so may result in the new node start serving request before synchronization with other node, e.g. start
  a second cluster.
* You will need to drop the node before restarting it. On a working Cassandra using docker exec run:
```
nodetool status
# get the Host ID of the node to remove
nodetool removenode 7fdd8c1a-5830-4796-ad69-a791523e64f9
```

For Redis, if you recreate a node (e.g. lost data, not just restart it), you will need to drop and re-add the node:
* First find the node id of the lost node, using:
  ```
  docker exec -ti squirreldb_ha_redis1_1 redis-cli cluster nodes
  ```
  It will be the node with "fail" and "noaddr"
* Drop this node on every Redis node:
  ```
  for i in 1 2 3 4 5 6; do docker exec squirreldb_ha_redis${i}_1 redis-cli cluster forget $NODE_ID;done
  ```
* Add the new node (for example, assuming lost node is the 172.28.0.26 and 172.28.0.21 is a working node):
  ```
  docker exec squirreldb_ha_redis_init_1 redis-cli --cluster add-node 172.28.0.26:6379 172.28.0.21:6378 --cluster-slave
  ```

For grafana, if you recreate it you will lose all your dashboard and must recreate them, but metric data are not lost.

For Prometheus and SquirrelDB are stateless. Prometheus also store recent metric data (15 days by default), but will query SquirrelDB store
when its local store is empty. So restarting/recreating them as no impact.
