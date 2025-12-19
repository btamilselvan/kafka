package com.success;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class KafkaSpringbootApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaSpringbootApplication.class, args);
	}

}
