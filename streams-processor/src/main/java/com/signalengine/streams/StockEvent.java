package com.signalengine.streams;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StockEvent {

    private String ticker;

    @JsonProperty(required = true)
    private double price;

    private long timestamp;

    public StockEvent() {}

    public String getTicker() { return ticker; }
    public double getPrice() { return price; }
    public long getTimestamp() { return timestamp; }
}
