package com.success.service;

import com.success.model.MessageDto;
import java.time.Duration;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KStreamProcessor {

  @Value("${kafka.order-service-topic-name}")
  private String orderServiceTopicName;

  public void toUpperCase(KStream<String, MessageDto> stream) {
    log.info("Starting KStream toUpperCase processing...");
    stream
        .mapValues(
            value -> {
              value.setMessage(value.getMessage().toUpperCase());
              return value;
            })
        .to(orderServiceTopicName);
    log.info("KStream toUpperCase processing completed.");
  }

  public void countWords(KStream<String, MessageDto> stream) {
    log.info("Starting KStream countWords processing...");
    stream
        .flatMapValues(value -> Arrays.asList(value.getMessage().toLowerCase().split("\\W+")))
        .selectKey((key, value) -> value) // the word as key
        .groupByKey()
        .count(Materialized.as("word-counts-store"))
        .toStream()
        .map(
            (key, count) ->
                KeyValue.pair(key, "{\"key\":\"" + key + "\", \"count\":" + count + "}"))
        .to(orderServiceTopicName, Produced.with(Serdes.String(), Serdes.String()));
    log.info("KStream countWords processing completed.");
  }

  public void processWithWindow(KStream<String, MessageDto> stream) {
    log.info("Starting KStream processing...");
    // stream.mapValues(value -> value.toUpperCase()).to(destinationTopicName);

    // word count example
    stream
        .flatMapValues(value -> Arrays.asList(value.getMessage().toLowerCase().split("\\W+")))
        .selectKey((key, value) -> value) // the word as key
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
        .count(Materialized.as("word-counts-store"))
        .toStream()
        .map(
            (windowedKey, count) ->
                KeyValue.pair(
                    windowedKey.key(),
                    "{\""
                        + windowedKey.key()
                        + "\":"
                        + count
                        + ", \"windowStart\":\""
                        + windowedKey.window().startTime()
                        + "\", \"windowEnd\":\""
                        + windowedKey.window().endTime()
                        + "\"}"))
        .to(orderServiceTopicName, Produced.with(Serdes.String(), Serdes.String()));

    // stream
    //     .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
    //     .selectKey((key, value) -> value) // the word as key
    //     .groupByKey()
    //     // .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
    //     .count(Materialized.as("word-counts-store"))
    //     .toStream()
    //     .map(
    //         (key, count) ->
    //             KeyValue.pair(
    //                 key,
    //                 "{\"key\":\""
    //                     + key
    //                     + "\", \"count\":"
    //                     + count
    //                     + "}"))
    //     .to(destinationTopicName, Produced.with(Serdes.String(), Serdes.String()));

    log.info("KStream processing completed.");
  }

  // join example can be added here
  public MessageDto processJoinStreams(
      MessageDto paymentServiceMessage, MessageDto stockServiceMessage) {
    var dto = new MessageDto();
    dto.setMessage(paymentServiceMessage.getMessage() + " - " + stockServiceMessage.getMessage());
    return dto;
  }
}
