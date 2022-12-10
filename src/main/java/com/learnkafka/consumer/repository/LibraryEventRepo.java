package com.learnkafka.consumer.repository;

import com.learnkafka.domain.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

public interface LibraryEventRepo extends CrudRepository<LibraryEvent,Integer> {

}
