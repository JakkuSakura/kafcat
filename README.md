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

2. Make sure Kafka is running (We assume at `localhost:9092`). You can use`docker-compose -f tests/plaintext-server.yml up`(note that kafka in docker on MacOS is problematic, I couldn't even setup the proper testing environment via docker)

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

8. Detailed help can be found at `./kafcat --help`, `./kafcat -C --help`, `./kafcat -P --help`, etc. You can also check out [kafkacat](https://github.com/edenhill/kafkacat) for reference. 

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

## Programming Style
- `git rebase` onto master branch when possible
- Squash commits before push
