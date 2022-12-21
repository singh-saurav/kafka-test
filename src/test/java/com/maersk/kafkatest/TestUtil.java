package com.maersk.kafkatest;

import static com.maersk.kafkatest.KafkaContainerTest.kafkaContainer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.maersk.TestEVent;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * <description>
 * </p>
 *
 * @author : sauravsingh
 * @created: 20/12/22.
 */
@Slf4j
public class TestUtil {

	public static KafkaProducer<String, TestEVent> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://testUrl");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkatest");
		return new KafkaProducer<>(props);
	}

	public static KafkaConsumer<String, TestEVent> createEventConsumer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://testUrl");
		props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkatest");
		return new KafkaConsumer<>(props);
	}

	public static void sendEvent(KafkaProducer producer, ProducerRecord record) throws ExecutionException, InterruptedException {

		Future sendFuture = producer.send(record);

		RecordMetadata metadata = (RecordMetadata) sendFuture.get();
		log.info("RecordMetadata topic: {}, offset: {}, partition: {}", metadata.topic(), metadata.offset(), metadata.partition());
	}

}
