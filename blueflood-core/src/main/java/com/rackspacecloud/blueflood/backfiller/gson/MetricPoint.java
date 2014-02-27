package com.rackspacecloud.blueflood.backfiller.gson;

import java.util.Map;

public class MetricPoint {
    private int metricType;
    private double valueDbl;
    private long valueI64;
    private int valueI32;
    private String valueStr;
    
    public MetricPoint(Map<String, ?> values) {
        metricType = asInteger(values.get("metricType"));
        valueDbl = asDouble(values.get("valueDbl"));
        valueI64 = asLong(values.get("valueI64"));
        valueI32 = asInteger(values.get("valueI32"));
        valueStr = asString(values.get("valueStr"));
    }
    
    public Class<?> getType() {
        if (metricType == 'L' || metricType == 'l')
            return Long.class;
        else if (metricType == 'I' || metricType == 'i')
            return Integer.class;
        else if (metricType == 'n')
            return Double.class;
        else if (metricType == 's' || metricType == 'b')
            return String.class;
        else throw new RuntimeException("Unexpected metric type " + (char)metricType);
    }
    
    public Object getValue() {
        if (metricType == 'L' || metricType == 'l')
            return valueI64;
        else if (metricType == 'I' || metricType == 'i')
            return valueI32;
        else if (metricType == 'n')
            return valueDbl;
        else if (metricType == 's' || metricType == 'b')
            return valueStr;
        else throw new RuntimeException("Unexpected metric type " + (char)metricType);
    }
    
    private static Long asLong(Object o) {
        if (o instanceof Long)
            return (Long)o;
        else if (o instanceof Integer)
            return ((Integer)o).longValue();
        else if (o instanceof Double)
            return (long)((Double) o).doubleValue();
        else if (o instanceof String)
            return Long.parseLong(o.toString());
        else 
            throw new RuntimeException("Cannot conver type");
    }
    
    private static Integer asInteger(Object o) {
        if (o instanceof Long)
            return ((Long) o).intValue();
        else if (o instanceof Integer)
            return (Integer)o;
        else if (o instanceof Double)
            return (int)((Double) o).doubleValue();
        else if (o instanceof String)
            return Integer.parseInt(o.toString());
        else 
            throw new RuntimeException("Cannot conver type");
    }
    
    private static Double asDouble(Object o) {
        if (o instanceof Long)
            return (double)((Long) o).longValue();
        else if (o instanceof Integer)
            return (double)((Integer) o).intValue(); 
        else if (o instanceof Double)
            return (Double)o;
        else if (o instanceof String)
            return Double.parseDouble(o.toString());
        else 
            throw new RuntimeException("Cannot conver type");
    }
    
    private static String asString(Object o) {
        return o == null ? "" : o.toString();
    }
}
