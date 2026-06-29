package com.signalengine.producer;

public class StockEvent {

    private String ticker;
    private double price;
    private long timestamp;

    public StockEvent() {}

    public StockEvent(String ticker, double price, long timestamp) {
        this.ticker = ticker;
        this.price = price;
        this.timestamp = timestamp;
    }

    public String getTicker() { return ticker; }
    public double getPrice() { return price; }
    public long getTimestamp() { return timestamp; }
}
