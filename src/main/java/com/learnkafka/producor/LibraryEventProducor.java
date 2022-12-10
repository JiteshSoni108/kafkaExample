package com.learnkafka.producor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class LibraryEventProducor {

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    private String topicName="library-events";
    @Autowired
    private ObjectMapper objectMapper;

    /*
    This method will call async call.
    */
    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key=libraryEvent.getLibrarayEventID();
        String value=objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer,String> producerRecord = buildProducerRecord(key, value, topicName);
        //With producerRecord with header
        CompletableFuture<SendResult<Integer, String>> send = kafkaTemplate.send(producerRecord);
        //With topic and key & Value
        //CompletableFuture<SendResult<Integer, String>> send = kafkaTemplate.send(topicName, key, value);
        /*
        Here , get() method will act as synchronous calling
        try {
            SendResult<Integer, String> integerStringSendResult = send.get(1,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }*/
        send.whenComplete((result , ex) ->{
            if (ex == null) {
                log.info("Kafka worked , message {} " , result.getProducerRecord());
            }
            else {
                log.error("data {} , record {} , error{} ",result.getRecordMetadata() , result.getProducerRecord() , ex.getMessage());
            }
        });
        System.out.print("outPut send "+send);
    }

    //This method will build the Producer record for send message
    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    
}
