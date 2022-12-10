package com.learnkafka.consumer.repository;

import com.learnkafka.domain.FailureRecord;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface FailureRecordRepo extends CrudRepository<FailureRecord, Integer> {
    /**
     * This method will return failed record by status
     */
    List<FailureRecord> findAllByStatus(String status);
}
