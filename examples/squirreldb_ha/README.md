# SquirrelDB HA setup

This setup provide both scalability and availability of a Prometheus remote store using SquirrleDB.

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
  - "cassanra1:9042"
  - "cassanra2:9042"
  # Replication factor should be odd, since SquirrelDB rely on quorum for it's consistency
  replication_factor: 3
redis:
  # Like for Cassandra addresses, you just need to have enough addresses to connected to one
  # working Redis node, from here client will discover all nodes in the cluster.
  addresses: "redis1:6379,redis2:6379"
```

## Quickstart

This quickstart will start a SquirrelDB in HA (with Cassandra cluster and Redis) using docker-compose.
Obvisouly this is only for testing since all components will run on the same machine.

Like the single-node quickstart, this will also start a Prometheus + Grafana and one node_exporter to
have some data in SquirrelDB.

Start the stack:

```
docker-compose up -d
```

Initialize Redis cluster:

```
docker exec -ti squirreldb_ha_redis1_1 redis-cli --cluster create --cluster-replicas 1 $(docker inspect -f "{{ .NetworkSettings.Networks.squirreldb_ha_default.IPAddress}}:6379" squirreldb_ha_redis{1,2,3,4,5,6}_1)

```

Then as for single SquirrelDB, go to http://localhost:3000 and:

* Add a Prometheus datasource (using http://prometheus:9090 as URL)
* Create your dashboard or import a dashboard (for example import dashboard with ID 1860).

### Testing availability

To test availability, you can stop+start or remove/recreate component.

Example of test:

```
# Recreate Prometheus, losing its local store of metrics. This ensure that metric points
# are read from SquirrelDB
docker-compose up -d --force-recreate --renew-anon-volumes prometheus

# Kill one Cassandra then restart it
docker-compose stop -t0 cassandra1
docker-compose start cassandra1

# Stop and recreate one SquirrelDB
docker-compose rm --force --stop -v squirreldb2
docker-compose up -d squirreldb2

# Note: the above may require a restart of nginx if SquirrelDB changed its IP
docker-compose restart nginx

docker-compose stop -t0 redis4
docker-compose up -d redis4
docker-compose stop -t0 redis2
docker-compose up -d redis2
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

For grafana, if you recreate it you will lose all your dashboard and must recreate them.

For Prometheus appart from not gathering metrics while down, you don't lose anything since SquirrelDB store
all metrics data.

Redis cluster have few drawback when running in containers: it don't really like changing IP addresses. Some known issue:
* If a Redis node take the IP of another node (e.g. they swap their IP), it may be needed to run some "cluster meet" so
  the cluster re-discover all nodes
* Still in the case of a node taking the IP of another node, a slave may replicate from the wrong node. In this case restart the slave.
* Still in the case of a node taking the IP of another node, a previous master may start as empty master (a master with 0 slots). In this case the easier is to
  drop the node ("docker-compose stop --force --stop -v redisN") and follow step to re-add a lost node.
* If all Redis node change their IPs, it will be needed to run "cluster meet" or the cluster won't find other node

The "cluster meet" is a redis-cli command that will perform an handshake between two nodes and make both of them aware of each other.
Since the cluster gossip to propagate known node, if one node known all nodes, then the gossip will make all nodes known all nodes.

Example of a cluster meet:
```
for i in 2 3 4 5 6; do docker exec -ti squirreldb_ha_redis1_1 redis-cli cluster meet $(docker inspect -f "{{ .NetworkSettings.Networks.squirreldb_ha_default.IPAddress}}" squirreldb_ha_redis2_1) 6379;done

```

Still for Redis (a similar to Cassandra), if you recreate a node (e.g. lost data, not just restart it), you will need to drop and re-add the node:
* Fist find the node id of the lost node, using:
  ```
  docker exec -ti squirreldb_ha_redis1_1 redis-cli cluster nodes
  ```
  It will be the node with "fail" and "noaddr"
* Drop this node on every Redis node:
  ```
  for i in 1 2 3 4 5 6; do docker exec squirreldb_ha_redis${i}_1 redis-cli cluster forget $NODE_ID;done
  ```
  Find the node ID with: "docker exec -ti squirreldb_ha_redis1_1 redis-cli cluster nodes"
* Add the new node:
  ```
  working_node=1
  new_node=6  # this assume you deleted and recreated redis6
  docker exec squirreldb_ha_redis${working_node}_1 redis-cli --cluster add-node $(docker inspect -f "{{ .NetworkSettings.Networks.squirreldb_ha_default.IPAddress}}:6379" squirreldb_ha_redis${new_node}_1) $(docker inspect -f "{{ .NetworkSettings.Networks.squirreldb_ha_default.IPAddress}}:6379" squirreldb_ha_redis${working_node}_1) --cluster-slave
  ```

For SquirrelDB it's stateless, and could be killed without fear of data lose. However be aware that if SquirrelDB change
its IP, nginx won't update it and you may need to restart nginx.
