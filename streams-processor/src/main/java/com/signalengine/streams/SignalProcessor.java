package com.signalengine.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class SignalProcessor {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "signal-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream("stock-prices", Consumed.with(Serdes.String(), Serdes.String()))
            // pull just the price out of the JSON
            .mapValues(SignalProcessor::extractPrice)
            .filter((ticker, price) -> price != null)
            // group by ticker key, then look at a 60-second window
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60)))
            // state = "openPrice,currentPrice" stored as a plain string
            .aggregate(
                () -> "-1,0",
                (ticker, price, state) -> {
                    String[] parts = state.split(",");
                    double open = Double.parseDouble(parts[0]);
                    String newOpen = open < 0 ? price : parts[0]; // lock in first price as open
                    return newOpen + "," + price;
                },
                Materialized.with(Serdes.String(), Serdes.String())
            )
            .toStream()
            // only pass through windows where price dropped >= 2% from open
            .filter((windowedKey, state) -> {
                String[] parts = state.split(",");
                double open = Double.parseDouble(parts[0]);
                double current = Double.parseDouble(parts[1]);
                return open > 0 && current <= open * 0.98;
            })
            // format a signal payload
            .map((windowedKey, state) -> {
                String[] parts = state.split(",");
                double open = Double.parseDouble(parts[0]);
                double current = Double.parseDouble(parts[1]);
                double dropPct = (open - current) / open * 100;

                String signal = String.format(
                    "{\"ticker\":\"%s\",\"open\":%.2f,\"current\":%.2f,\"dropPct\":\"%.2f%%\"}",
                    windowedKey.key(), open, current, dropPct
                );
                return KeyValue.pair(windowedKey.key(), signal);
            })
            .to("price-signals", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();

        System.out.println("Signal processor running — watching for 2% drops in 60s windows...");
    }

    private static String extractPrice(String json) {
        try {
            return String.valueOf(mapper.readValue(json, StockEvent.class).getPrice());
        } catch (Exception e) {
            return null;
        }
    }
}
