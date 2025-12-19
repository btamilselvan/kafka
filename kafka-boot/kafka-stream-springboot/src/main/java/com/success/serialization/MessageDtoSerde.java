package com.success.serialization;

import com.success.model.MessageDto;
import org.apache.kafka.common.serialization.Serdes;

public class MessageDtoSerde extends Serdes.WrapperSerde<MessageDto> {
  public MessageDtoSerde() {
    super(new CustomSerializer(), new CustomDeserializer());
  }
}
