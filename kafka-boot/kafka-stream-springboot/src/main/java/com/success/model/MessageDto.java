package com.success.model;

import java.time.Instant;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MessageDto {
  private String message;
  private Instant timestamp;
}
