package com.success.serialization;

import com.success.model.MessageDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

@Slf4j
public class MessageDtoSerializer implements Serializer<MessageDto> {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public byte[] serialize(String topic, MessageDto data) {
    if (data == null) {
      return new byte[0];
    }
    log.info("Serializing message: {}", data.getMessage());
    return objectMapper.writeValueAsBytes(data);
  }
}
