**Overview**
- **Project:** Kafka Streams example that demonstrates consuming messages, joining streams, and producing derived messages (order stream and word-counts).
- **Main components:** `com.success.config.KafkaStreamConfig` (topology + Serdes), `com.success.service.KStreamProcessor` (processing logic), custom `MessageDto` Serde under `com.success.serialization`.

**Configuration**
- **Properties:** set in `src/main/resources/application.yaml` (or environment)
  - `kafka.bootstrap-server`: bootstrap servers (comma separated)
  - `kafka.application-id`: Kafka Streams application id
  - `kafka.payment-service-topic-name`: source topic A (payment)
  - `kafka.stock-service-topic-name`: source topic B (stock)
  - `kafka.order-service-topic-name`: output topic for joined/processed messages

**What changed / current behavior**
- **Serdes:** Project uses a `MessageDtoSerde` (custom serializer/deserializer) for `MessageDto` objects. Ensure producers send JSON matching `MessageDto` fields (`message`, `timestamp`).
- **Join handling:** Streams are filtered to drop null values and `selectKey` is used to set a non-null, deterministic key (derived from `MessageDto.message`) so joins will not skip records due to null keys.
- **Word-count processing:** `KStreamProcessor` provides multiple processors:
  - `toUpperCase`: maps `MessageDto.message` to uppercase and forwards to `order-service-topic`.
  - `countWords`: non-windowed word count that produces records with value JSON like `{\"key\":\"<word>\", \"count\":<n>}`.
  - `processWithWindow`: windowed (1-minute) word count that produces JSON values like `{\"<word>\":<count>, \"windowStart\":\"...\", \"windowEnd\":\"...\"}` and writes messages with the word as the key.

**How to run**
- Build and run the Spring Boot Kafka Streams app from the project root:
  ```bash
  ./mvnw -DskipTests spring-boot:run
  ```

**Testing - produce sample messages**
- Example `MessageDto` JSON (value only). Use `kafka-console-producer` to send to the payment/stock topics:
  ```bash
  kafka-console-producer --broker-list localhost:29092 --topic my-payment-service-topic
  {"message":"Hello Kafka world","timestamp":"2025-12-16T01:00:00Z"}
  ```

**Testing - consume output**
- Consume the `order-service-topic` (or the configured output topic) with the console consumer:
  ```bash
  kafka-console-consumer --bootstrap-server localhost:29092 \
    --topic my-order-service-topic --from-beginning --property print.key=true
  ```
- For the word-count output (string JSON values) the default console consumer will print readable JSON values like `{\"hello\":3}` or `{\"key\":\"hello\", \"count\":3}` depending on which processor is active.

**Notes**
- The join won't work if message keys are different for the topics (streams) being joined.
- Ensure the timestamps in the sample messages are within the join window (5 seconds) for matching records to be joined.