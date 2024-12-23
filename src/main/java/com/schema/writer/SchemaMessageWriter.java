package com.schema.writer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class SchemaMessageWriter implements AutoCloseable {
    private final String bootstrapServers;
    private final String topic;
    private final Schema schema;
    private final String registryUrl;
    private final KafkaProducer<String, GenericRecord> producer;
    private final SchemaFieldGenerator fieldGenerator;

    public SchemaMessageWriter(SchemaConfig config) {
        this.bootstrapServers = config.getBootstrapServers();
        this.topic = config.getTopic();
        this.registryUrl = config.getSchemaRegistry();
        this.schema = new Schema.Parser().parse(config.getSchemaJson());
        this.producer = createProducer();
        this.fieldGenerator = new SchemaFieldGenerator();
    }

    private KafkaProducer<String, GenericRecord> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);    
        props.put("schema.registry.url", registryUrl);
        return new KafkaProducer<>(props);
    }

    public CompletableFuture<Void> sendMessage() {
        GenericRecord record = createRandomRecord();
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        producer.send(new ProducerRecord<>(topic, record), (metadata, exception) -> {
            if (exception != null) {
                future.completeExceptionally(exception);
            } else {
                System.out.printf("[%s] Successfully sent message to partition=%d, offset=%d%n",
                    topic, metadata.partition(), metadata.offset());
                System.out.printf("Record content: %s%n", record);
                future.complete(null);
            }
        });
        
        return future;
    }

    public void sendMessages(int count, long delayMillis) throws InterruptedException {
        for (int i = 0; i < count; i++) {
            sendMessage();
            if (delayMillis > 0) {
                Thread.sleep(delayMillis);
            }
        }
    }

    private GenericRecord createRandomRecord() {
        GenericRecord record = fieldGenerator.generateRandomRecord(schema);
        return record;
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
