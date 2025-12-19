package com.success.config;

import com.success.model.MessageDto;
import com.success.serialization.CustomDeserializer;
import com.success.serialization.MessageDtoSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaConfigSecondary {

  @Value("${kafka.bootstrap-server}")
  private String bootStrapServer;

  @Value("${kafka.consumer.secondary-group-id}")
  private String consumerGroupId;

  @Value("${kafka.consumer.order-service-group-id}")
  private String orderServiceGroupId;

  @Bean
  public ConsumerFactory<String, MessageDto> consumerFactorySecondary() {
    // Configuration code for ConsumerFactory
    Map<String, Object> configProps = new HashMap<>();
    configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);

    // configProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    configProps.put(ConsumerConfig.GROUP_ID_CONFIG, orderServiceGroupId);

    configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class);
    configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new DefaultKafkaConsumerFactory<>(configProps);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, MessageDto>
      kafkaListenerContainerFactorySecondary() {
    ConcurrentKafkaListenerContainerFactory<String, MessageDto> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactorySecondary());
    factory.setConcurrency(3);
    return factory;
  }

  @Bean
  public ProducerFactory<String, MessageDto> producerFactorySecondary() {
    // Configuration code for ProducerFactory
    Map<String, Object> configProps = new HashMap<>();
    configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
    configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageDtoSerializer.class);
    return new DefaultKafkaProducerFactory<>(configProps);
  }

  @Bean
  public KafkaTemplate<String, MessageDto> secondaryKafkaTemplate() {
    return new KafkaTemplate<>(producerFactorySecondary());
  }
}
