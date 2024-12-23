package com.schema.writer;

public class SchemaConfig {
    private String bootstrapServers;
    private String topic;
    private String schemaJson;
    private String schemaRegistry;

    public SchemaConfig(String bootstrapServers, String topic, String schemaJson, String schemaRegistry) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.schemaJson = schemaJson;
        this.schemaRegistry = schemaRegistry;
    }

    public static SchemaConfigBuilder builder() {
        return new SchemaConfigBuilder();
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public String getSchemaJson() {
        return schemaJson;
    }

    public String getSchemaRegistry() {
        return schemaRegistry;
    }

    public static class SchemaConfigBuilder {
        private String bootstrapServers = "localhost:9092";
        private String topic;
        private String schemaRegistry;
        private String schemaJson;

        public SchemaConfigBuilder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public SchemaConfigBuilder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public SchemaConfigBuilder schemaJson(String schemaJson) {
            this.schemaJson = schemaJson;
            return this;
        }

        public SchemaConfigBuilder schemaRegistry(String schemaRegistry) {
            this.schemaRegistry = schemaRegistry;
            return this;
        }

        public SchemaConfig build() {
            if (topic == null || topic.isEmpty()) {
                throw new IllegalArgumentException("Topic must not be null or empty");
            }
            if (schemaJson == null || schemaJson.isEmpty()) {
                throw new IllegalArgumentException("Schema JSON must not be null or empty");
            }
            if (schemaRegistry == null || schemaRegistry.isEmpty()) {
                throw new IllegalArgumentException("Schema registry URL must not be null or empty");
            }
            return new SchemaConfig(bootstrapServers, topic, schemaJson, schemaRegistry);
        }
    }
}
