# Signal Engine

An event-driven signal detection system built with Java and Kafka. Generates a synthetic stock price feed and fires alerts when a ticker drops 2% within a 60-second tumbling window.

## Architecture

```
Producer → [stock-prices topic] → Streams Processor → [price-signals topic] → Consumer
```

- **Producer** — generates synthetic stock price events (random walk, plus an optional forced crash via `--crash`) at high volume
- **Streams Processor** — detects a 2% price drop within a 60-second window
- **Consumer** — receives and prints signals

## Design notes

**Data source:** events are synthetically generated, not pulled from a real market-data feed. A random walk perturbs each ticker's price every tick, with an optional `--crash` flag to force a deterministic drop for testing.

**Window strategy:** tumbling (non-overlapping) 60-second windows, keyed by ticker. Tumbling was chosen over sliding because each window only needs to capture one open price and compare it to the latest price — a sliding window would recompute overlapping aggregates for no benefit here, at extra state-store cost. 60 seconds balances catching a fast, flash-crash-style drop against false positives from normal tick-to-tick noise.

## Prerequisites

- Java 17+
- Maven
- Docker

## Running locally

**1. Start Kafka**
```bash
docker compose up
```

**2. Build all modules**
```bash
mvn package -DskipTests
```

**3. Run each piece in its own terminal**
```bash
java -jar streams-processor/target/streams-processor-1.0-SNAPSHOT.jar
java -jar consumer/target/consumer-1.0-SNAPSHOT.jar
java -jar producer/target/producer-1.0-SNAPSHOT.jar
```

## Testing a signal

Use the `--crash` flag to force a ticker down 3% after a 2-second warmup:

```bash
java -jar producer/target/producer-1.0-SNAPSHOT.jar --crash AAPL
```

Expected output in the consumer terminal:
```
[SIGNAL] {"ticker":"AAPL","open":150.00,"current":145.50,"dropPct":"3.00%"}
```

## Open issues

| # | Description | Label |
|---|-------------|-------|
| 1 | Fat JAR: modules can't run without bundled dependencies | bug |
| 2 | Brittle state encoding in SignalProcessor | enhancement |
| 3 | Missing StockEvent model class | enhancement |

