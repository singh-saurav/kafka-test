package com.maersk.kafkatest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
@EmbeddedKafka(topics = {"test.topic"}, partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092", "group.id=test" })
class KafkaTestApplicationTests {
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	@Test
	void contextLoads() {
		System.out.println("Sending message ");
		kafkaTemplate.send("test.topic", "Sending with our own simple KafkaProducer");
	}

	@KafkaListener(topics = "test.topic", groupId = "group.id")
	public void receive(ConsumerRecord<?, ?> consumerRecord) {
		System.out.println("received payload= " + consumerRecord.toString());
	}
}
