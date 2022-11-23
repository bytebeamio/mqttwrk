# mqttwrk
Wrk/Wrk2 inspired tool for conformance and scale testing mqtt brokers

### Examples
------------

- Run 10K connection producing
  - total of 20 messages
  - 1 publisher per connection
  - 0 subscribers per connection
  - 1 message/second frequency per publisher

```bash
cargo run --release -- bench -n 20 -a 1 -b 0 -r 1
```
