package com.rackspacecloud.blueflood.KafkaProducer;

import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import kafka.producer.KeyedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class KafkaProductionWork implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaProductionWork.class);
    private ArrayList<String> messageQueue = new ArrayList<String>();
    private String eventName;
    private Integer numberOfProducers;

    KafkaProductionWork(List rollUps, String eName, Integer noOfProducers) {
        messageQueue.addAll(rollUps);
        eventName = eName;
        numberOfProducers = noOfProducers;
    }

    @Override
    public void run() {
            ArrayList<KeyedMessage<String,String>> messageList = new ArrayList<KeyedMessage<String, String>>();
            for(String message : messageQueue) {
                messageList.add(new KeyedMessage<String, String>(eventName,message));
            }

            //select a random producer from the list and send the messages
            log.debug("Sending messages to the topic "+eventName);
            KafkaProducerServiceStarter.producerList.get(new Random().nextInt(numberOfProducers)).send(messageList);
    }
 }

