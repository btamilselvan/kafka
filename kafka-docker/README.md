## Kafka (Docker) — Overview & Notes

This document provides a concise overview of Apache Kafka concepts that are relevant when running Kafka in Docker (or similar containerized environments). It covers architecture, networking/listener configuration, KRaft controller quorum guidance, and a short note about Spring Kafka's `ConcurrentKafkaListenerContainerFactory`.

### Overview

Apache Kafka is a distributed streaming platform and durable event store for real-time event streaming. A Kafka cluster consists of one or more brokers that host topics, which are split into partitions for scalability and parallelism.

### Control Plane (Cluster Management)

- **ZooKeeper vs KRaft:** Historically, Kafka used Apache ZooKeeper for cluster metadata and coordination (leader election, configuration). Newer Kafka versions support KRaft (Kafka Raft) which embeds the control plane inside brokers and removes the external ZooKeeper dependency.
- **Role:** The control plane maintains cluster metadata (brokers, topics, partition leader assignments).

### Data Flow & Scaling

- **Producers:** Publish messages to topics. Partitioning strategies (key-based, round-robin) determine distribution across partitions.
- **Partitions:** Store messages immutably and provide ordering guarantees per partition.
- **Consumers & Consumer Groups:** Each partition is consumed by a single consumer instance within a consumer group, enabling parallel processing. Multiple consumer groups can independently read the same topic for different use cases.

### Example: User Activity Tracking

- **Producer (tracking service):** Writes events such as `PAGE_VIEW` to topic `user_activity_raw`. Using `user_id` as the partition key preserves ordering for a single user's events.
- **Consumers (analytics / recommendations):** Independent microservices use different consumer group IDs (e.g., `analytics-group`, `recommendation-group`) to consume the same stream concurrently.

### Listeners & Networking (Docker-specific notes)

Kafka clients initially connect to a broker (bootstrap). The broker then returns addresses (advertised listeners) for each broker so the client can connect directly. In containerized setups, you commonly need two kinds of listeners:

- **Internal listener:** Used for inter-broker communication and connections from containers on the same Docker network (e.g., `INTERNAL://kafka:29092`).
- **External listener:** Used for clients on the host or external machines (e.g., `EXTERNAL://localhost:9092`).

Key environment variables:

```
KAFKA_LISTENERS=INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093

KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka:29092,EXTERNAL://localhost:9092

KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
```

- **`KAFKA_LISTENERS`**: Where the broker binds network interfaces (actual sockets). Use `0.0.0.0` inside containers to accept connections from any interface.
- **`KAFKA_ADVERTISED_LISTENERS`**: What the broker tells clients to use when connecting back. These must be reachable by the client (container DNS names for internal clients; `localhost` or host IP for external clients).
- **`KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`**: Maps listener names to security protocols (e.g., `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`).

Notes:
- In single-node Docker setups, you can often use a single `localhost` listener. In multi-node setups, each broker must advertise addresses reachable by clients and other brokers.

### KRaft Controller Quorum (Recommendation)

KRaft relies on a quorum of controllers for consensus on metadata. Use an odd number of controller nodes (3, 5, 7, ...) to maximize fault tolerance per node added.

Example (N=5 controllers):

```
Total nodes (N): 5
Majority (M): floor(N/2)+1 = 3
Max tolerated failures (F): N - M = 2
```

Choosing an odd number avoids wasted hardware without improving fault tolerance (e.g., moving from 3→4 does not increase the number of tolerated failures).

### Security / Protocol Mapping

- Use `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` to map each listener to a protocol. Example:

```
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:SSL
```

### Spring Kafka: ConcurrentKafkaListenerContainerFactory

`ConcurrentKafkaListenerContainerFactory` is part of Spring Kafka and creates listener containers capable of running multiple concurrent Kafka message listeners. Use it when you need higher consumer throughput or parallelism in processing.

### Quick Tips

- **Bootstrap/connectivity:** Ensure `KAFKA_ADVERTISED_LISTENERS` uses addresses resolvable by the clients that will connect.
- **Docker port mapping examples:** Broker ports often map to host ports like `29092`, `39092`, `49092` for multiple brokers.
- **KRaft deployment:** Prefer 3 or 5 controller nodes for production-grade fault tolerance.

### References

- Kafka listeners and networking: https://kafka.apache.org/documentation/#networking
- KRaft overview: https://kafka.apache.org/documentation/#kraft
- Spring Kafka concurrency: https://spring.io/projects/spring-kafka
- Consumer poll frequency: https://codemia.io/knowledge-hub/path/is_there_a_way_to_configure_polling_interval_of_kafkalistener
