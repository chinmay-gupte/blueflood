package com.rackspacecloud.blueflood.backfiller.gson;

public class Metric<T> {
    private String name;
    private T value;
    
    public Metric(String name, T value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public T getValue() {
        return value;
    }
}
