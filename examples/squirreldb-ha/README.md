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
  address: "redis:6379"
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

For Redis since it's currently a single node, a brutal shutdown may lost recent data and a recreation is likely to
lose last 15 minutes of data.

For SquirrelDB it's stateless, and could be killed without fear of data lose. However be aware that if SquirrelDB change
its IP, nginx won't update it and you may need to restart nginx.
