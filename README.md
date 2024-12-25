# Schema Message Writer

> A demo utility tool for AutoMQ, designed to generate and send messages based on Avro schemas. This tool helps demonstrate AutoMQ's Kafka-compatible features by providing an easy way to produce test messages that conform to specified Avro schemas.


## Features

- Generate random data based on Avro schema definitions
- Integrate with Kafka Schema Registry
- Configure message count and sending intervals
- Command-line interface support

## Installation

```bash
git clone https://github.com/AutoMQ/schema-message-writer.git
cd schema-message-writer
mvn clean install
```

## Quick Start

1. Start Kafka and Schema Registry
2. Run the application:

```bash
java -jar target/schema-message-writer-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -b localhost:9092 \
  -r http://localhost:8081 \
  -t your-topic \
  -s schema.json
```

## Configuration

| Option | Description | Default |
|--------|-------------|---------|
| -b, --bootstrap-servers | AutoMQ bootstrap servers | localhost:9092 |
| -r, --schema-registry | Schema Registry URL | Required |
| -t, --topic | Kafka topic name | Required |
| -s, --schema | Schema JSON file/string | Required |
| -c, --count | Number of messages | 1 |
| -d, --delay | Delay between messages (ms) | 1000 |
