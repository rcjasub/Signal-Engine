# Signal Engine

An event-driven signal detection system built with Java and Kafka. Simulates a high-volume stock price feed and fires alerts when a ticker drops 2% within a 60-second window.

## Architecture

```
Producer → [stock-prices topic] → Streams Processor → [price-signals topic] → Consumer
```

- **Producer** — fires fake stock price events at high volume
- **Streams Processor** — detects a 2% price drop within a 60-second window
- **Consumer** — receives and prints signals

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

