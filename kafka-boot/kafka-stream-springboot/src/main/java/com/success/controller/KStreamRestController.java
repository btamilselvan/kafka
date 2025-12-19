package com.success.controller;

import java.time.Instant;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.success.model.MessageDto;

@RestController
public class KStreamRestController {

    @GetMapping("/ping")
    public String ping() {
        return Instant.now().toString();
    }

    public String createMessage(MessageDto messageDto) {
        return "Message received with ID: " + messageDto.getMessage() + " and Content: " + messageDto.getTimestamp();
    }
}
