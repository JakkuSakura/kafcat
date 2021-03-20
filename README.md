# kcat

a Rust port of kafkacat

## Basic

1. Make sure Kafka is running (We assume at `localhost:9092`)
2. Setup the listener,

```
cargo run -- consume --topic test
```

3. Setup the producer in another terminal,

```
cargo run -- produce --topic test
```

4. Type any key and value with the default delimiter `:`. For example, `hello:world`.

5. You should see `hello:world` in the consumer terminal.
