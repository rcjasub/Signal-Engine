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
        String crashTicker = parseCrashTicker(args);
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

        if (crashTicker != null) {
            System.out.println("[CRASH MODE] Will force " + crashTicker + " down 3% after 2s warmup");
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Producer started — sending events to '" + TOPIC + "'...");

            while (true) {
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
}
