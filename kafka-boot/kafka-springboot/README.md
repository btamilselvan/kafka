**Kafka Spring Boot Example**

This repository contains a small Spring Boot example that demonstrates producing and consuming messages with Apache Kafka. It includes a sample REST controller and a basic producer/consumer setup you can build and run locally or in an IDE.

**Quick Summary**
- **Purpose:**: Lightweight demo for integrating Spring Boot with Kafka (producer + consumer).
- **Main class:**: `com.success.KafkaSpringbootApplication` (run from your IDE or via the wrapper).
- **Port:**: default Spring Boot port `8080` (unless overridden by configuration).

**Prerequisites**
- **JDK:**: Java 17+ installed (project has been run with Java 21 in local debug sessions).
- **Maven:**: The project ships with the Maven wrapper (`mvnw`), so you do not need a separate Maven installation.
- **Kafka:**: A running Kafka cluster (local or remote) is required to exercise producer/consumer functionality.

**Build**
- From the project root run:

```
./mvnw clean package
```

or to install to your local repository:

```
./mvnw install
```

**Run**
- Run from the command line:

```
./mvnw spring-boot:run
```

- Or run the packaged jar (after `package`):

```
java -jar target/*.jar
```

- Run from the IDE by launching `com.success.KafkaSpringbootApplication` (the debug session in this workspace used a direct `java` invocation with JDWP JVM args).

**What’s included / notable code**
- **Controller(s):**: A sample `KafkaController` provides REST endpoints to produce messages to Kafka.
- **Producer / Consumer:**: Simple configuration and examples to demonstrate producer and consumer flows.
- **Configuration:**: Kafka connection settings (bootstrap servers, topics, serializer/deserializer) are defined in `application.yml` / `application.properties` (check `src/main/resources`).

**Tips & Troubleshooting**
- If you see connection errors, verify `spring.kafka.bootstrap-servers` points to a reachable Kafka cluster.
- Ensure topic names match between producer and consumer.
- Use the included Maven wrapper to avoid local Maven version issues.

