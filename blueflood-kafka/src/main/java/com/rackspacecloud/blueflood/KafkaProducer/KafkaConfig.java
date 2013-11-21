package com.rackspacecloud.blueflood.KafkaProducer;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

class KafkaConfig {

    private static Properties kafkaProps = new Properties();

    static{
        try {
            init();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void init() throws IOException {
        String configStr = System.getProperty("kafka.config");
        if (configStr != null) {
            URL configUrl = new URL(configStr);
            kafkaProps.load(configUrl.openStream());
        }
    }

    public static Properties getKafkaProperties() {
        return kafkaProps;
    }

    public static Integer getIntegerProperty(String propertyName) {
        return Integer.parseInt(kafkaProps.getProperty(propertyName));
    }


}
