package com.rackspacecloud.blueflood.KafkaProducer;


import com.github.nkzawa.emitter.Emitter;
import junit.framework.Assert;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.mockito.Mockito.*;


public class KafkaProducerServiceStarterTest {

    Emitter eventEmitter = new Emitter();
    String eventName = "TEST";
    String testEvent = "testEvent";

    KafkaProducerServiceStarter kafkaServiceStarterSpy;
    Producer mockProducer;

    @Before
    public void setUp() {
        KafkaProducerServiceStarter kafkaServiceStarter = new KafkaProducerServiceStarter(eventEmitter,eventName);
        kafkaServiceStarterSpy = spy(kafkaServiceStarter);

        //Create a spy producer object and add it to the producer list
        mockProducer = mock(Producer.class);
        ArrayList<Producer> producerList = kafkaServiceStarterSpy.getProducerList();
        producerList.clear();
        producerList.add(mockProducer);
    }

    @Test
    public void testKafkaServiceStarter() throws Exception {
        //Start KafkaProduction and test whether listener object was added
        kafkaServiceStarterSpy.startKafkaProduction();
        Assert.assertEquals(eventEmitter.listeners(eventName).contains(kafkaServiceStarterSpy), true);

        //Emit an event
        eventEmitter.emit(eventName, testEvent);
        //Verify that the call method was called atleast once
        verify(kafkaServiceStarterSpy, timeout(2000).atLeastOnce()).call(testEvent);

        //Sleep and wait for execution to complete
        Thread.sleep(2000);

        //Test whether executor actually executed a task
        Assert.assertEquals(kafkaServiceStarterSpy.getKafkaExecutors().getCompletedTaskCount(), 1);

        //Test whether send method od producer was called atleast once
        ArrayList<KeyedMessage<String,String>> messageList = new ArrayList<KeyedMessage<String, String>>();
        messageList.add(new KeyedMessage<String, String>(eventName,testEvent));
        verify(mockProducer, timeout(5000).atLeastOnce()).send(messageList);

        //Stop Kafka Production and test whether the listener object was removed
        kafkaServiceStarterSpy.stopKafkaProduction();
        Assert.assertEquals(eventEmitter.listeners(eventName).contains(kafkaServiceStarterSpy), false);


    }
}
