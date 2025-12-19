package com.success.service;

import com.success.model.MessageDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ConsumerService {

  @KafkaListener(topics = "${kafka.topic-name}", groupId = "${kafka.consumer.group-id}")
  public void consumeMessage(String message) {
    // Logic to process the consumed message
    log.info("Consumed message: {}", message);
  }

  // another topic with a different group id and a listener container factory
  @KafkaListener(
      containerFactory = "kafkaListenerContainerFactorySecondary",
      topics = "${kafka.secondary-topic-name}",
      groupId = "${kafka.consumer.secondary-group-id}")
  public void consumeSecondaryMessage(MessageDto message) {
    // Logic to process the consumed message
    log.info("Consumed message from another topic: {}", message.getMessage());
  }

  // order-service topic listener
  @KafkaListener(
      containerFactory = "kafkaListenerContainerFactorySecondary",
      topics = "${kafka.order-service-topic-name}",
      groupId = "${kafka.consumer.order-service-group-id}")
  public void consumeOrderServiceMessage(MessageDto message) {
    // Logic to process the consumed message
    log.info("Consumed message from order-service topic: {}", message.getMessage());
  }
}