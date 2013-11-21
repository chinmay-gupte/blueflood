package com.rackspacecloud.blueflood.KafkaProducer;

import com.github.nkzawa.emitter.Emitter;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.List;
import java.util.ArrayList;

import java.util.Arrays;
import java.util.concurrent.*;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaProducerServiceStarter implements Emitter.Listener {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerServiceStarter.class);
    //We want to use the batching capabilities of the Kafka producer objects, so we store a static list
    //and reuse the same objects in the work queue instead of creating new ones. Arraylist is preferred
    //due to O(1) for get operations
    static ArrayList<Producer> producerList = new ArrayList<Producer>();
    //Unbounded queue of kafka production tasks
    private static final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
    private static ThreadPoolExecutor kafkaExecutors;
    //Switch to tell if threadpoolexecutor and producers were instantiated properly
    private static boolean ready = false;
    private static final Integer DEFAULT_KAFKA_PRODUCERS = 10;
    private static Integer number_of_producers; //Takes value from kafka config if specified

    //Per Instance variables
    private Emitter eventEmitter;
    private String event;

    static {
        try {
            number_of_producers = KafkaConfig.getIntegerProperty("number_of_producers") != null ? KafkaConfig.getIntegerProperty("number_of_producers") : DEFAULT_KAFKA_PRODUCERS;
            //Executors working to distribute messages to the producer list
            //"Item 69: Prefer concurrency utilities to wait and notify"- Joshua Bloch,Effective Java 2nd edition
            kafkaExecutors = new ThreadPoolExecutor (
                    number_of_producers*2,
                    number_of_producers*2,
                    30, TimeUnit.SECONDS,
                    workQueue,
                    Executors.defaultThreadFactory(),
                    new ThreadPoolExecutor.AbortPolicy()
            );
            //Fill the producer list with objects. Is there any other way to do this?
            for(int i=0; i<number_of_producers; i++) {
              Producer producer = new Producer(new ProducerConfig(KafkaConfig.getKafkaProperties()));
              producerList.add(producer);
            }
            //We are ready to rumble
            ready = true;
        } catch(Exception e) {
            log.error("Error encountered while initializing the Kafka Service", e);
            //Takes care of case wherein, initialization threw an exception after thread pool was created
            if(kafkaExecutors != null && !kafkaExecutors.isShutdown()) {
              kafkaExecutors.shutdownNow();
            }
        }
    }

    public KafkaProducerServiceStarter(Emitter emitter, String event) {
        this.eventEmitter = emitter;
        this.event = event;
    }


    public void startKafkaProduction() {
      //Check if initialization was successful
      if(ready == false) {
        log.error("Could not start kafka production due to errors during initialisation phase.");
        return;
      }
      //KafkaProduction was already started on this listener
      if(eventEmitter.listeners(this.event).contains(this)) {
        log.warn("Kafka Production already started");
        return;
      }

      //Register with the event emitter
      eventEmitter.on(this.event, this);
      log.debug("Listening to event "+this.event);
    }


    public void stopKafkaProduction() {
      //Check to see of the kafka production was already stopped
      if(!eventEmitter.listeners(this.event).contains(this)) {
        log.debug("Kafka Production is already shutdown");
        return;
      } else {
        /* TODO: Do we need this block of code to shutdown the executors?
        if(!kafkaExecutors.isTerminating() || !kafkaExecutors.isShutdown()) {
            log.debug("Shutting down after terminating all tasks");
            //Stop the executors
            kafkaExecutors.shutdown();
            //Wait for certain time to terminate thread pool safely.
            try {
              kafkaExecutors.awaitTermination(10,TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              log.debug("Thread interrupted while waiting for safe termination of thread pool executor");
              //Stop the kafka executors abruptly. TODO : Think about the consequences?
              kafkaExecutors.shutdownNow();
            }
        }
        */
        //Un-subscribe from event emitter
        eventEmitter.off(this.event, this);
        log.debug("Stopped listening to event "+this.event);
      }
    }


    @Override
    public void call(Object... objects) {
        List events  = Arrays.asList(objects);
        kafkaExecutors.execute(new KafkaProductionWork(events, this.event, this.number_of_producers));
    }

    //Used only for tests
    static ThreadPoolExecutor getKafkaExecutors() {
        return kafkaExecutors;
    }

    //Used only for tests
    static ArrayList<Producer> getProducerList() {
        return producerList;
    }

}
