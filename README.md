# kafcat

Kafcat is a Rust fully async rewrite of [kafkacat](https://github.com/edenhill/kafkacat).

I was trying to copy some data from kafka on a remote server to localhost for testing purpose. Kafkacat was the only usable tool to do so. However, it is written in C and does not support binary data at all. I was working on a pull request, but it haven't been merged yet, and the code was hard to maintain and error prone. So I wrote my own kafcat.

## Features

It mimicks kafkacat's cli usage, but with extra features. It can

- read from stdin and send to kafka topic
- read from kafka to stdin
- copy from one topic to another(even on different servers)
- import/export data in json format
- swappable backend(only librdkafka for now, kafka-rust coming soon)

## Drawbacks

Kafcat is still on early development, so

- does not support TLS yet

- does not support Apache AVRO

- does not support complex input/output format

  

## Basic Usage

1. Compile kafcat. You can find the binary at target/release/kafcat

   ```bash
   cargo build --release
   ```

2. Make sure Kafka is running (We assume at `localhost:9092`). You can use the `docker-compose.yml` below via `docker-compose up`

   ```yaml
   ---
   version: '2'
   services:
     zookeeper:
       image: confluentinc/cp-zookeeper:6.1.0
       hostname: zookeeper
       container_name: zookeeper
       ports:
         - "2181:2181"
       environment:
         ZOOKEEPER_CLIENT_PORT: 2181
         ZOOKEEPER_TICK_TIME: 2000
   
     broker:
       image: confluentinc/cp-kafka:6.1.0
       hostname: broker
       container_name: broker
       depends_on:
         - zookeeper
       ports:
         - "29092:29092"
         - "9092:9092"
         - "9101:9101"
       environment:
         KAFKA_BROKER_ID: 1
         KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
         KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
         KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
         KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
         KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
         KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
         KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
         KAFKA_JMX_PORT: 9101
         KAFKA_JMX_HOSTNAME: localhost
   
   
   ```

   

3. Setup the listener, assuming `kafcat` is in current directory

   ```bash
   ./kafcat -C --topic test
   ```

4. Setup the producer in another terminal,

   ```bash
   ./kafcat -P --topic test
   ```

5. Copy a topic. the format is `./kafcat copy <from> -- <to>`, `from` and `to` are exactly as used consumer and producer

   ```
   ./kafcat copy --topic test -- --topic test2
   ```

6. Type any key and value with the default delimiter `:`. For example, `hello:world`.

7. You should see `hello:world` in the consumer terminal.

8. Detailed help can be found at `./kafcat --help`, `./kafcat -C --help`, `./kafcat -P --help`, etc. You can also check out  [kafkacat](https://github.com/edenhill/kafkacat) for reference. 

   ```text
   kafcat-consume 
   
   USAGE:
       kafcat {consume, -C} [FLAGS] [OPTIONS] --topic <topic>
   
   FLAGS:
       -e, --exit
               Exit successfully when last message received
   
       -h, --help
               Prints help information
   
       -V, --version
               Prints version information
   
   
   OPTIONS:
       -b, --brokers <brokers>
               Broker list in kafka format [default: localhost:9092]
   
       -s, --format <format>
               Serialize/Deserialize format [default: text]
   
       -G, --group-id <group-id>
               Consumer group id. (Kafka >=0.9 balanced consumer groups) [default: kafcat]
   
       -K <key-delimiter>
               Delimiter to split input key and message [default: :]
   
       -D <msg-delimiter>
               Delimiter to split input into messages(currently only supports '\n') [default: 
               ]
   
       -o <offset>
               Offset to start consuming from:
                                    beginning | end | stored |
                                    <value>  (absolute offset) |
                                    -<value> (relative offset from end)
                                    s@<value> (timestamp in ms to start at)
                                    e@<value> (timestamp in ms to stop at (not included))[default:
               beginning]
   
       -p, --partition <partition>
               Partition
   
       -t, --topic <topic>
               Topic
   kafcat-produce 
   
   USAGE:
       kafcat {produce, -P} [OPTIONS] --topic <topic>
   
   FLAGS:
       -h, --help       Prints help information
       -V, --version    Prints version information
   
   OPTIONS:
       -b, --brokers <brokers>        Broker list in kafka format [default: localhost:9092]
       -s, --format <format>          Serialize/Deserialize format [default: text]
       -G, --group-id <group-id>      Consumer group id. (Kafka >=0.9 balanced consumer groups)
                                      [default: kafcat]
       -K <key-delimiter>             Delimiter to split input key and message [default: :]
       -D <msg-delimiter>             Delimiter to split input into messages(currently only supports
                                      '\n') [default: 
                                      ]
       -p, --partition <partition>    Partition
       -t, --topic <topic>            Topic
   kafcat-copy 
   Copy mode accepts two parts of arguments <from> and <to>, the two parts are separated by [--].
   <from> is the exact as Consumer mode, and <to> is the exact as Producer mode.
   
   USAGE:
       kafcat copy <from>... [--] <to>...
   
   ARGS:
       <from>...    
       <to>...      
   
   FLAGS:
       -h, --help       Prints help information
       -V, --version    Prints version information
   
   
   ```

