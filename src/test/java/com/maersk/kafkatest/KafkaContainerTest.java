package com.maersk.kafkatest;

import static com.maersk.kafkatest.TestUtil.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.BeforeClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import com.maersk.TestEVent;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * <description>
 * </p>
 *
 * @author : sauravsingh
 * @created: 20/12/22.
 */
@SpringBootTest //(properties = { "client.ping.timeout=60", "docker.client.strategy=org.testcontainers.dockerclient.UnixSocketClientProviderStrategy" })
@Slf4j
public class KafkaContainerTest {

	protected static Network network = Network.newNetwork();

	private KafkaConsumer<String, TestEVent> consumer;

	private KafkaProducer<String, TestEVent> producer;

	static final String TOPIC = "test-topic";

	@BeforeClass
	void setup() {
//		Startables.deepStart(Stream.of(kafkaContainer)).join();
	}

	@BeforeEach
	void prepare() {
		consumer = createEventConsumer();
		consumer.subscribe(Collections.singleton(TOPIC));
		Duration duration = Duration.ofSeconds(5);
		consumer.poll(duration);
		producer = createProducer();
	}

	@AfterEach
	void cleanup() {
		producer.close();
		consumer.close();
	}

	protected static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.1"))
			.withEmbeddedZookeeper().withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9093 ,BROKER://0.0.0.0:9092")
			.withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
			.withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER").withEnv("KAFKA_BROKER_ID", "1")
			.withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1").withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
			.withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1").withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
			.withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "").withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
			.withNetwork(network);

//	static {
//
//	}

	@Test
	void testEventCreatingUserInRepo() throws ExecutionException, InterruptedException {
		assumeTrue(kafkaContainer.isRunning());
		String newUserId = UUID.randomUUID().toString();
		TestEVent event = TestEVent.newBuilder().setUsername(newUserId).setTweet("Testing Tweet").setTimestamp(System.nanoTime()).build();

		ProducerRecord<String, TestEVent> record = new ProducerRecord<>(TOPIC, UUID.randomUUID().toString(), event);

		sendEvent(producer, record);

		ConsumerRecord<String, TestEVent> singleRecord = KafkaTestUtils.getSingleRecord(consumer, TOPIC);

		log.info("Received Event " + singleRecord.value());
		assertTrue(newUserId.equals(singleRecord.value().getUsername()));
	}

	static {
		Startables.deepStart(Stream.of(kafkaContainer)).join();
	}
}
