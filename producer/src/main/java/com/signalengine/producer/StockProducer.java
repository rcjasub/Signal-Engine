package com.signalengine.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class StockProducer {

    private static final String TOPIC = "stock-prices";
    private static final String[] TICKERS = {"AAPL", "GOOGL", "MSFT", "AMZN"};
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // run with: java -jar producer.jar --crash AAPL
        // or load-test with: java -jar producer.jar --duration 600
        String crashTicker = parseCrashTicker(args);
        long durationSeconds = parseDuration(args);
        boolean crashFired = false;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "5");
        props.put("batch.size", "65536");

        double[] prices = {150.0, 140.0, 300.0, 185.0};
        Random random = new Random();
        long count = 0;
        long startTime = System.currentTimeMillis();
        long endTime = durationSeconds > 0 ? startTime + (durationSeconds * 1000) : Long.MAX_VALUE;

        if (crashTicker != null) {
            System.out.println("[CRASH MODE] Will force " + crashTicker + " down 3% after 2s warmup");
        }
        if (durationSeconds > 0) {
            System.out.println("[LOAD TEST] Running for " + durationSeconds + "s, then reporting throughput");
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Producer started — sending events to '" + TOPIC + "'...");

            while (System.currentTimeMillis() < endTime) {
                for (int i = 0; i < TICKERS.length; i++) {
                    boolean isCrashTarget = crashTicker != null
                        && !crashFired
                        && TICKERS[i].equals(crashTicker)
                        && System.currentTimeMillis() - startTime > 2000;

                    if (isCrashTarget) {
                        prices[i] *= 0.97; // force a 3% drop — guaranteed to trigger the signal
                        crashFired = true;
                        System.out.printf("[CRASH] Forced %s down 3%% → $%.2f%n", crashTicker, prices[i]);
                    } else {
                        prices[i] *= (1 + (random.nextDouble() - 0.5) * 0.005);
                    }

                    StockEvent event = new StockEvent(
                        TICKERS[i],
                        Math.round(prices[i] * 100.0) / 100.0,
                        System.currentTimeMillis()
                    );

                    producer.send(new ProducerRecord<>(TOPIC, TICKERS[i], mapper.writeValueAsString(event)));
                    count++;
                }

                if (count % 10_000 == 0) {
                    System.out.printf("Sent %,d events%n", count);
                }
            }

            if (durationSeconds > 0) {
                double elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000.0;
                System.out.printf(
                    "%n[LOAD TEST COMPLETE] Sent %,d events in %.1fs -> %.0f events/sec%n",
                    count, elapsedSeconds, count / elapsedSeconds
                );
            }
        }
    }

    private static String parseCrashTicker(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--crash")) {
                return (i + 1 < args.length) ? args[i + 1].toUpperCase() : "AAPL";
            }
        }
        return null;
    }

    private static long parseDuration(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--duration")) {
                return (i + 1 < args.length) ? Long.parseLong(args[i + 1]) : 0;
            }
        }
        return 0;
    }
}
