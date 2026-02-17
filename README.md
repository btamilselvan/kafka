# Apache Kafka Learning Repository

A comprehensive collection of Apache Kafka implementations demonstrating core concepts, KRaft mode deployment, and Kafka Streams processing with Spring Boot.

## 📁 Project Structure

```
kafka/
├── kafka-docker/              # KRaft-based Kafka cluster (Docker Compose)
│   ├── docker-compose.yaml    # 3 controllers + 3 brokers setup
│   └── README.md
├── kafka-boot/
│   ├── kafka-springboot/      # Producer/Consumer with Spring Boot
│   │   ├── src/
│   │   ├── pom.xml
│   │   └── README.md
│   └── kafka-stream-springboot/  # Kafka Streams processing
│       ├── src/
│       ├── pom.xml
│       └── README.md
└── README.md                  # This file
```

## 🎯 What's Implemented

### 1. KRaft-Based Kafka Cluster (`kafka-docker/`)
- **3 Controller nodes** - Separate control plane for metadata management
- **3 Broker nodes** - Data plane for message storage and serving
- **Dual listeners** - Internal (inter-broker) and external (host access)
- **Production-ready architecture** - Fault-tolerant quorum configuration

### 2. Spring Boot Producer/Consumer (`kafka-boot/kafka-springboot/`)
- REST API for producing messages
- Consumer service with message processing
- Custom serialization/deserialization
- Multiple Kafka configurations (primary/secondary)
- MessageDto model with JSON serialization

### 3. Kafka Streams Application (`kafka-boot/kafka-stream-springboot/`)
- Stream joining (payment + stock topics)
- Word count processing (windowed and non-windowed)
- Custom Serdes for MessageDto
- Stream transformations (uppercase, filtering)
- REST endpoints for stream queries

---

## 📚 Core Kafka Concepts

- Apache Kafka is a distributed data streaming platform and durable event store for streaming real-time data events. The architecture consists of a Kafka Cluster composed of one or more Kafka Brokers (servers) hosting Topics, which are logically divided into one or more Partitions. The cluster's Control Plane (its metadata and coordination layer) is managed either by the legacy ZooKeeper or the modern KRaft system. Events are stored immutably in the partitions. Producers use Partitioning Strategies (like Key-based or Round-Robin) to distribute messages across partitions, determining the load balance among consumers.

### Example: User Activity Tracking Pipeline

In a real-world scenario like User Activity Tracking (capturing clicks, page views, etc.), the pipeline uses Kafka to decouple services and enable real-time analysis:

Producer (Tracking Service): A Spring Boot service acts as the Producer, capturing user events (e.g., PAGE_VIEW) and writing them to a topic (e.g., user_activity_raw). It uses the user_id as the Partition Key to ensure all events from the same user are processed in order.

Partitions: The topic is highly partitioned (e.g., 100 partitions) to distribute the massive I/O load across the Kafka cluster.

Consumers (Decoupled Services): Multiple, independent microservices subscribe to the same topic, each using a unique Consumer Group ID:

The Real-Time Analytics Service (Group ID: analytics-group) calculates live metrics.

The Recommendation Engine (Group ID: recommendation-group) processes the same events to update user profiles.

- The Key Rule in Action: Each of the 100 partitions is assigned to one instance in the analytics-group and simultaneously to one instance in the recommendation-group. This allows both services to process the full stream in parallel and independently, guaranteeing that every event is seen and processed by both applications without duplication within their respective groups. The durability of Kafka allows any consumer to recover and replay missed messages after a failure.

### The Control Plane (Cluster Management):

The cluster's operational integrity relies on a distributed coordination service to maintain consensus and manage metadata. Historically, this role was filled by Apache ZooKeeper, which is used for essential tasks like configuration management, service discovery, leader election, and distributed locking among the Kafka Brokers. In modern Kafka versions, this control plane functionality is now integrated directly into the brokers using the KRaft protocol, eliminating the external dependency on ZooKeeper. The chosen manager (ZooKeeper or KRaft) maintains the cluster's state metadata (brokers, topics, and partition leader assignments).

### Data Flow and Scaling:
Events are stored immutably in the partitions. Producers use Partitioning Strategies (like Key-based or Round-Robin) to distribute messages across partitions, determining the load balance among consumers. To consume this data, Consumer Groups are used: each partition is assigned to exactly one consumer instance within a given Consumer Group, enabling both parallel processing and independent consumption by multiple services.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Kafka Cluster                            │
│                                                             │
│  ┌──────────────┐         ┌──────────────┐                │
│  │ Controller 1 │         │  Broker 1    │                │
│  │ Controller 2 │────────▶│  Broker 2    │                │
│  │ Controller 3 │  Meta   │  Broker 3    │                │
│  │  (KRaft)     │  data   │  (Topics)    │                │
│  └──────────────┘         └──────────────┘                │
│         ▲                         ▲                         │
│         │                         │                         │
└─────────┼─────────────────────────┼─────────────────────────┘
          │                         │
    ┌─────┴──────┐           ┌─────┴──────┐
    │ Producers  │           │ Consumers  │
    └────────────┘           └────────────┘
```

### Topics & Partitions

- **Topic**: Logical channel for messages (e.g., `user-activity`, `orders`)
- **Partition**: Physical subdivision of a topic for parallelism
- **Ordering**: Guaranteed within a partition, not across partitions
- **Replication**: Each partition can have multiple replicas for fault tolerance

**Example:**
```
Topic: user-activity (3 partitions)
├── Partition 0: [msg1, msg4, msg7, ...]
├── Partition 1: [msg2, msg5, msg8, ...]
└── Partition 2: [msg3, msg6, msg9, ...]
```

### Producers

- Publish messages to topics
- **Partitioning strategies**:
  - **Key-based**: Same key → same partition (preserves ordering per key)
  - **Round-robin**: Distribute evenly when no key provided
  - **Custom**: Implement custom partitioner logic

**Key-based partitioning example:**
```java
// All messages with same user_id go to same partition
producer.send(new ProducerRecord<>("user-activity", userId, event));
```

### Consumers & Consumer Groups

- **Consumer**: Reads messages from topic partitions
- **Consumer Group**: Multiple consumers working together
- **Partition assignment**: Each partition consumed by ONE consumer in a group
- **Parallel processing**: Multiple groups can read same topic independently

**Scaling pattern:**
```
Topic: orders (6 partitions)
Consumer Group: order-processing (3 consumers)
├── Consumer 1: Partitions 0, 1
├── Consumer 2: Partitions 2, 3
└── Consumer 3: Partitions 4, 5

Consumer Group: analytics (2 consumers)
├── Consumer 1: Partitions 0, 1, 2
└── Consumer 2: Partitions 3, 4, 5
```

### Message Retention

- **Time-based**: Keep messages for N days (default: 7 days)
- **Size-based**: Keep up to N GB per partition
- **Compaction**: Keep only latest value per key (for changelog topics)

---

## 🚀 KRaft Mode (Kafka Raft)

### What is KRaft?

KRaft replaces Apache ZooKeeper with a built-in consensus protocol for managing Kafka cluster metadata.

**Benefits:**
- Simpler deployment (no external ZooKeeper cluster)
- Faster metadata operations
- Better scalability (millions of partitions)
- Reduced operational complexity

### Architecture

```
Traditional (ZooKeeper):          KRaft Mode:
┌────────────┐                   ┌────────────┐
│ ZooKeeper  │                   │Controller 1│
│  Cluster   │◀──────────────────│Controller 2│
└────────────┘                   │Controller 3│
      ▲                          │ (Embedded) │
      │                          └────────────┘
┌─────┴──────┐                         ▲
│   Brokers  │                         │
└────────────┘                   ┌─────┴──────┐
                                 │   Brokers  │
                                 └────────────┘
```

### Controller Quorum

KRaft uses Raft consensus requiring a majority (quorum) for decisions.

**Quorum sizing:**
| Controllers | Majority Needed | Max Failures Tolerated |
|-------------|-----------------|------------------------|
| 1           | 1               | 0                      |
| 3           | 2               | 1                      |
| 5           | 3               | 2                      |
| 7           | 4               | 3                      |

**Best practice**: Use odd numbers (3, 5, 7) to maximize fault tolerance per node.

### Process Roles

- **controller**: Manages metadata only (no data)
- **broker**: Stores and serves data only
- **controller,broker**: Combined mode (not recommended for production)

**Our setup** (`kafka-docker/`):
- 3 dedicated controllers (nodes 1-3)
- 3 dedicated brokers (nodes 4-6)

### Key Configuration

```yaml
KAFKA_PROCESS_ROLES: controller          # or broker, or both
KAFKA_NODE_ID: 1                         # Unique ID per node
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@controller-1:9093,2@controller-2:9093,3@controller-3:9093
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
```

---

## 🌊 Kafka Streams

### What is Kafka Streams?

A client library for building real-time stream processing applications that read from and write to Kafka topics.

**Key characteristics:**
- Processes data record-by-record (not micro-batches)
- Stateful and stateless operations
- Exactly-once processing semantics
- Fault-tolerant state stores
- No separate cluster needed (runs as part of your app)

### Stream Processing Topology

```
Source Topic(s)
      ↓
  [Filter]
      ↓
   [Map]
      ↓
  [GroupBy]
      ↓
 [Aggregate]
      ↓
Sink Topic(s)
```

### Core Abstractions

#### KStream (Event Stream)
- Represents an unbounded stream of records
- Each record is independent
- Suitable for: logs, events, transactions

```java
KStream<String, MessageDto> stream = builder.stream("input-topic");
stream.filter((key, value) -> value.getMessage() != null)
      .mapValues(value -> value.getMessage().toUpperCase())
      .to("output-topic");
```

#### KTable (Changelog Stream)
- Represents a table (latest value per key)
- Updates replace previous values
- Suitable for: user profiles, product catalog, aggregations

```java
KTable<String, Long> wordCounts = stream
    .flatMapValues(value -> Arrays.asList(value.split(" ")))
    .groupBy((key, word) -> word)
    .count();
```

#### GlobalKTable
- Replicated to all application instances
- Used for reference data (lookups)
- No partitioning constraints for joins

### Stream Operations

**Stateless operations** (no local state):
- `filter`, `filterNot` - Conditional filtering
- `map`, `mapValues` - Transform records
- `flatMap`, `flatMapValues` - One-to-many transformation
- `branch` - Split stream into multiple streams
- `merge` - Combine multiple streams

**Stateful operations** (maintain local state):
- `groupBy`, `groupByKey` - Group records by key
- `count`, `reduce`, `aggregate` - Compute aggregations
- `join` - Combine two streams/tables
- `windowing` - Time-based grouping

### Windowing

Group events into time-based windows for aggregations.

**Window types:**
- **Tumbling**: Fixed-size, non-overlapping (e.g., every 5 minutes)
- **Hopping**: Fixed-size, overlapping (e.g., 5-min window, advance 1 min)
- **Sliding**: Dynamic size based on record timestamps
- **Session**: Dynamic windows based on inactivity gaps

```java
// Tumbling window: count words every 1 minute
stream.groupByKey()
      .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
      .count();
```

### Stream Joins

Combine two streams based on keys and time windows.

**Join types:**
- **Inner join**: Emit when both sides have matching key
- **Left join**: Emit for left side, null if no right match
- **Outer join**: Emit for both sides, null for missing matches

```java
// Join payment and stock streams within 5-second window
KStream<String, PaymentDto> payments = builder.stream("payments");
KStream<String, StockDto> stocks = builder.stream("stocks");

payments.join(stocks,
    (payment, stock) -> new OrderDto(payment, stock),
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5))
);
```

### State Stores

Kafka Streams maintains local state for stateful operations.

**Types:**
- **In-memory**: Fast but lost on restart
- **RocksDB**: Persistent, survives restarts
- **Changelog topic**: Backup in Kafka for recovery

**State store features:**
- Automatically backed up to Kafka
- Restored on application restart
- Partitioned across application instances

---

## Palin Kafka vs Streams
| Feature      | Plain Kafka Consumer (and Producer)                                                                                        | Kafka Streams                                                                                                                                                           |
|--------------|----------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Role         | Data Transport (Read/Write)                                                                                                | Data Transformation & Aggregation                                                                                                                                       |
| Architecture | A simple client that reads or writes raw bytes. Focuses on message delivery and offset management.                         | A powerful client library that implements stream processing concepts (like joins, windows, state stores) and manages its own internal consumers, producers, and state.  |
| State        | Stateless. If it needs state (e.g., counting clicks), the state must be managed externally (e.g., in Redis or a database). | Stateful. Manages local, fault-tolerant, persistent state stores (backed by local disk and Kafka topics) for tasks like counting, windowing, and aggregation.           |
| Unit of Data | Records (individual messages).                                                                                             | Streams (KStream) and Tables (KTable).                                                                                                                                  |
| Complexity   | Low complexity (simple poll() loop).                                                                                       | Medium complexity (handles threading, local state, and fault tolerance automatically).                                                                                  |
| Example Use  | Reading messages and writing them to a database.                                                                           | Counting the number of unique users per minute (stateful aggregation).                                                                                                  |


## Simple Consumer vs. Kafka Streams
| Feature                                     | Plain Kafka Consumer + Producer                                                                                                                                               | Kafka Streams Library                                                                                                                                        |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Simple Transformation (Mapping)             | YES. Consumer reads, transforms, and a Producer writes the result to a new topic. (The "middleman" approach).                                                                 | YES. Simple .map() or .mapValues() operation.                                                                                                                |
| Stateful Processing (Aggregation, Counting) | NO (Hard). Requires maintaining an external database (Redis, Postgres) to store and update the running counts/sums. Introduces network latency and complexity.                | YES (Easy). State is managed locally in RocksDB and kept durable via internal changelog topics. Fast, reliable, and fault-tolerant by design.                |
| Joining Data                                | NO (Hard). Requires custom logic to buffer data from Topic A until the corresponding data arrives from Topic B. Complex to manage concurrency and memory.                     | YES (Easy). Built-in .join() and .leftJoin()operations handle the buffering, state management, and matching of records from different topics based on keys.  |
| Time-Based Logic (Windowing)                | NO (Hard). Requires custom code to track event time and manage memory for data that falls within a specific time frame (e.g., counting clicks only within a 5-minute window). | YES (Easy). Built-in .windowedBy() methods (Tumbling, Hopping, Session) handle all temporal logic and eviction of old state automatically.                   |
| Scalability & Resilience                    | Requires you to manage client threading, offset commits, and failure handling manually.                                                                                       | Built-in. Handles parallelization (one stream task per partition), automatic state recovery, and consumer group rebalancing for you.                         |


## 🛠️ Implementation Details

### Kafka Docker Setup

**Start the cluster:**
```bash
cd kafka-docker
docker-compose up -d
```

**Access points:**
- Broker 1: `localhost:29092`
- Broker 2: `localhost:39092`
- Broker 3: `localhost:49092`

**Verify cluster:**
```bash
docker ps  # Check all 6 containers running
docker logs broker-1  # Check broker logs
```

### Spring Boot Producer/Consumer

**Key components:**
- `KafkaController` - REST endpoints for producing messages
- `ProducerService` - Kafka message producer
- `ConsumerService` - Kafka message consumer with `@KafkaListener`
- `MessageDto` - Custom message model
- Custom serializers/deserializers

**Run application:**
```bash
cd kafka-boot/kafka-springboot
./mvnw spring-boot:run
```

**Produce message:**
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"message":"Hello Kafka","timestamp":"2025-01-15T10:00:00Z"}'
```

### Kafka Streams Application

**Key components:**
- `KafkaStreamConfig` - Stream topology definition
- `KStreamProcessor` - Processing logic (joins, word counts, windowing)
- `MessageDtoSerde` - Custom Serde for MessageDto
- `KStreamRestController` - REST endpoints for queries

**Processing pipelines:**
1. **Stream joining**: Combines payment + stock → order
2. **Word count (non-windowed)**: Continuous word frequency
3. **Word count (windowed)**: 1-minute tumbling window counts
4. **Uppercase transformation**: Maps messages to uppercase

**Run application:**
```bash
cd kafka-boot/kafka-stream-springboot
./mvnw spring-boot:run
```

**Test with console producer:**
```bash
# Produce to payment topic
kafka-console-producer --broker-list localhost:29092 \
  --topic my-payment-service-topic
{"message":"hello world kafka","timestamp":"2025-01-15T10:00:00Z"}

# Consume from order topic
kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic my-order-service-topic --from-beginning --property print.key=true
```

---

## 🔑 Key Learnings

### Kafka Fundamentals
- Topics are logical, partitions are physical
- Ordering guaranteed per partition only
- Consumer groups enable parallel processing
- Key-based partitioning preserves ordering per key

### KRaft Benefits
- Simplified operations (no ZooKeeper)
- Faster metadata operations
- Better scalability
- Odd-numbered controller quorum for optimal fault tolerance

### Kafka Streams Patterns
- Stateless operations for simple transformations
- Stateful operations for aggregations and joins
- Windowing for time-based analytics
- State stores automatically backed up to Kafka
- Join windows critical for stream-stream joins

### Spring Kafka Integration
- `@KafkaListener` for simple consumers
- `ConcurrentKafkaListenerContainerFactory` for parallel processing
- Custom Serdes for complex objects
- Stream topology defined in `@Configuration` classes

---

## 📖 References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [KRaft Mode Overview](https://kafka.apache.org/documentation/#kraft)
- [Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)
- [Spring for Apache Kafka](https://spring.io/projects/spring-kafka)
- [Kafka Listeners & Networking](https://kafka.apache.org/documentation/#networking)

---

**Built for learning Kafka, KRaft, and Kafka Streams with Spring Boot** ☕️
