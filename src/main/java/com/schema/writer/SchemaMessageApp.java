package com.schema.writer;

import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "schema-writer", 
        mixinStandardHelpOptions = true,
        version = "1.0",
        description = "Generates and sends schema messages to Kafka")
public class SchemaMessageApp implements Callable<Integer> {

    @Option(names = {"-b", "--bootstrap-servers"}, 
            description = "Kafka bootstrap servers", 
            defaultValue = "localhost:9092")
    private String bootstrapServers;

    @Option(names = {"-r", "--schema-registry"}, 
            description = "Schema registry URL")
    private String schemaRegistry;

    @Option(names = {"-t", "--topic"}, 
            description = "Kafka topic name", 
            required = true)
    private String topic;

    @Option(names = {"-s", "--schema"}, 
            description = "Schema JSON string or file path", 
            required = true)
    private String schema;

    @Option(names = {"-c", "--count"}, 
            description = "Number of messages to send", 
            defaultValue = "1")
    private int messageCount;

    @Option(names = {"-d", "--delay"}, 
            description = "Delay between messages in milliseconds", 
            defaultValue = "1000")
    private long delayMillis;

    @Override
    public Integer call() throws Exception {
        try {
            // read schema from file if it ends with .json
            String schemaJson;
            if (schema.endsWith(".json")) {
                schemaJson = new String(java.nio.file.Files.readAllBytes(
                    java.nio.file.Paths.get(schema)));
            } else {
                schemaJson = schema;
            }

            // create configuration object
            SchemaConfig config = SchemaConfig.builder()
                    .bootstrapServers(bootstrapServers)
                    .schemaRegistry(schemaRegistry)
                    .topic(topic)
                    .schemaJson(schemaJson)
                    .build();

            // create writer and send messages
            try (SchemaMessageWriter writer = new SchemaMessageWriter(config)) {
                System.out.printf("Starting to send %d messages to topic '%s'%n", 
                        messageCount, topic);
                System.out.printf("Bootstrap servers: %s%n", bootstrapServers);
                
                writer.sendMessages(messageCount, delayMillis);
                
                System.out.println("Successfully sent all messages!");
                return 0;
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            return 1;
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new SchemaMessageApp()).execute(args);
        System.exit(exitCode);
    }
}
