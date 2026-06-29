package com.signalengine.streams;

public class StockEvent {

    private String ticker;
    private double price;
    private long timestamp;

    public StockEvent() {}

    public String getTicker() { return ticker; }
    public double getPrice() { return price; }
    public long getTimestamp() { return timestamp; }
}
