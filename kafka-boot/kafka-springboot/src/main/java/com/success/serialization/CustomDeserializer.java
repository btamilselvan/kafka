package com.success.serialization;

import com.success.model.MessageDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.ObjectMapper;

@Slf4j
public class CustomDeserializer implements Deserializer<MessageDto> {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public MessageDto deserialize(String topic, byte[] data) {
    try {
      log.info("Deserializing message from topic: {}", topic);
      return objectMapper.readValue(data, MessageDto.class);
    } catch (Exception e) {
      throw new SerializationException("Error deserializing message", e);
    }
  }
}
