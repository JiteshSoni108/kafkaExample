package com.learnkafka.consumer.service;

import com.learnkafka.consumer.repository.FailureRecordRepo;
import com.learnkafka.domain.FailureRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailureService {
    private FailureRecordRepo failureRecordRepo;
    FailureService( FailureRecordRepo failureRecordRepo){
        this.failureRecordRepo=failureRecordRepo;
    }

    public void saveFailedRecord(ConsumerRecord<?,?> consumerRecord , Exception ex,String recordStatus){
        log.info(" saveFailedRecord started consumerRecord: {} , ex: {} ,status : {} , ",consumerRecord,ex,recordStatus);
        var failureRecord = new FailureRecord(null,consumerRecord.topic(),Integer.valueOf((String) consumerRecord.key()),String.valueOf(consumerRecord.value()),consumerRecord.partition(),consumerRecord.offset(),
                ex.getMessage(),recordStatus);
        failureRecordRepo.save(failureRecord);

    }


}
