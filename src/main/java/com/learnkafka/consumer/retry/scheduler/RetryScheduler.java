package com.learnkafka.consumer.retry.scheduler;

import com.learnkafka.config.LibraryEventConsumerConfig;
import com.learnkafka.consumer.repository.FailureRecordRepo;
import com.learnkafka.consumer.service.LibraryEventService;
import com.learnkafka.domain.FailureRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryScheduler {

    @Autowired
    LibraryEventService libraryEventService;

    @Autowired
    FailureRecordRepo failureRecordRepo;

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords() {
        log.info("Retrying Failed Records Started!");
        var status = LibraryEventConsumerConfig.RETRY;
        failureRecordRepo.findAllByStatus(status).forEach(failureRecord -> {
            try {
                var consumerRecord = buildConsumerRecord(failureRecord);
                libraryEventService.processLibEvent(consumerRecord);
                failureRecord.setStatus(LibraryEventConsumerConfig.SUCCESS);
            } catch (Exception e) {
                log.error("Exception in retryFailedRecords : ", e);
            }
        });
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(failureRecord.getTopic(),
                failureRecord.getPartition(), failureRecord.getOffset_value(), failureRecord.getKey(),
                failureRecord.getErrorRecord());
    }
}
