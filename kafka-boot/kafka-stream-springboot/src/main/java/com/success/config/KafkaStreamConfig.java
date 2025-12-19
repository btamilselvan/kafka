package com.success.config;

import com.success.model.MessageDto;
import com.success.serialization.MessageDtoSerde;
import com.success.service.KStreamProcessor;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

@Configuration
@EnableKafkaStreams
public class KafkaStreamConfig {

  @Value("${kafka.bootstrap-server}")
  private String bootStrapServer;

  @Value("${kafka.application-id}")
  private String applicationId;

  //payment-service
  @Value("${kafka.payment-service-topic-name}")
  private String paymentServiceTopicName;

  //stock-service
  @Value("${kafka.stock-service-topic-name}")
  private String stockServiceTopicName;

  //order-service
  @Value("${kafka.order-service-topic-name}")
  private String orderServiceTopicName;

  private final KStreamProcessor kStreamProcessor;

  public KafkaStreamConfig(KStreamProcessor kStreamProcessor) {
    this.kStreamProcessor = kStreamProcessor;
  }

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
    // Configuration code for Kafka Streams
    return new KafkaStreamsConfiguration(
        Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, applicationId,
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer,
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                WallclockTimestampExtractor.class.getName()));
  }

//   @Bean
  public KStream<String, MessageDto> kStream(StreamsBuilder streamsBuilder) {
    // Define your KStream processing topology here
    KStream<String, MessageDto> stream =
        streamsBuilder.stream(
            paymentServiceTopicName, Consumed.with(Serdes.String(), new MessageDtoSerde()));

    // this.kStreamProcessor.toUpperCase(stream);
    // this.kStreamProcessor.countWords(stream);

    this.kStreamProcessor.processWithWindow(stream);

    return stream;
  }

  //join example can be added here
  @Bean
  public KStream<String, MessageDto> kStreamJoin(
      StreamsBuilder streamsBuilder) {
    // Read payment stream, drop null values and set a non-null key derived from the value
    KStream<String, MessageDto> paymentStream =
      streamsBuilder
        .stream(paymentServiceTopicName, Consumed.with(Serdes.String(), new MessageDtoSerde()));
        // .filter((k, v) -> v != null && v.getMessage() != null)
        // .selectKey((k, v) -> v.getMessage());

    // Read stock stream, drop null values and set the same key strategy so join can match
    KStream<String, MessageDto> stockStream =
      streamsBuilder
        .stream(stockServiceTopicName, Consumed.with(Serdes.String(), new MessageDtoSerde()));
        // .filter((k, v) -> v != null && v.getMessage() != null)
        // .selectKey((k, v) -> v.getMessage());

    // Join the streams on the derived key
    paymentStream
      .join(
        stockStream,
        this.kStreamProcessor::processJoinStreams,
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)),
        StreamJoined.with(Serdes.String(), new MessageDtoSerde(), new MessageDtoSerde()))
      .to(orderServiceTopicName, Produced.with(Serdes.String(), new MessageDtoSerde()));

    return paymentStream;
  }
}
