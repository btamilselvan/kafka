package com.success.controller;

import com.success.service.ProducerService;
import java.time.Instant;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

  private final ProducerService producerService;

  public KafkaController(ProducerService producerService) {
    this.producerService = producerService;
  }

  @GetMapping("/ping")
  public String ping() {
    return Instant.now().toString();
  }

  // produce
  @GetMapping("/produce")
  public String produceMessage(@RequestParam String message) {
    producerService.sendMessage(message);
    return "Message sent to Kafka topic!";
  }

  @PostMapping("/produce")
  public String produceMessageSecondary(@RequestParam String message) {
    producerService.sendSecondaryMessage(message);
    return "Message sent to Kafka topic!";
  }
}
