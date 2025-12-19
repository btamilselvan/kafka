package com.success.service;

import com.success.model.MessageDto;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;

import org.apache.kafka.common.protocol.types.Field.Str;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ProducerService {

  @Value("${kafka.topic-name}")
  private String topicName;

  // payment-service
  @Value("${kafka.payment-service-topic-name}")
  private String paymentServiceTopicName;

  // stock-service
  @Value("${kafka.stock-service-topic-name}")
  private String stockServiceTopicName;

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final KafkaTemplate<String, MessageDto> secondaryKafkaTemplate;

  private final Random random = new Random();

  public ProducerService(
      KafkaTemplate<String, String> kafkaTemplate,
      @Qualifier("secondaryKafkaTemplate")
          KafkaTemplate<String, MessageDto> secondaryKafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
    this.secondaryKafkaTemplate = secondaryKafkaTemplate;
  }

  public void sendMessage(String message) {
    kafkaTemplate.send(topicName, message);
    log.info("Sent message to  {}: {}", topicName, message);
  }

  public void sendSecondaryMessage(String message) {

    //generate a random int key
    String key = String.valueOf(random.nextInt());
    MessageDto kafkaMessage = new MessageDto(message, java.time.Instant.now());

    secondaryKafkaTemplate.send(stockServiceTopicName, key, kafkaMessage);
    log.info("Sent message to {}: {}", stockServiceTopicName, message);

    sleep(1000); // slight delay to ensure order

    // key = String.valueOf(random.nextInt());
    secondaryKafkaTemplate.send(paymentServiceTopicName, key, kafkaMessage);
    log.info("Sent message to {}: {}", paymentServiceTopicName, message);
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Sleep interrupted", e);
    }
  }
}
