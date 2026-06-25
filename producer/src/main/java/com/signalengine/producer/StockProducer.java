package com.signalengine.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class StockProducer {

    private static final String TOPIC = "stock-prices";
    private static final String[] TICKERS = {"AAPL", "GOOGL", "MSFT", "AMZN"};
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "5");
        props.put("batch.size", "65536");

        // starting prices for each ticker
        double[] prices = {150.0, 140.0, 300.0, 185.0};
        Random random = new Random();
        long count = 0;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Producer started — sending events to '" + TOPIC + "'...");

            while (true) {
                for (int i = 0; i < TICKERS.length; i++) {
                    // drift price randomly up or down by up to 0.5% per tick
                    prices[i] *= (1 + (random.nextDouble() - 0.5) * 0.005);

                    Map<String, Object> event = Map.of(
                        "ticker", TICKERS[i],
                        "price", Math.round(prices[i] * 100.0) / 100.0,
                        "timestamp", System.currentTimeMillis()
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
}
