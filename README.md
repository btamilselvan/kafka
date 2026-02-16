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
