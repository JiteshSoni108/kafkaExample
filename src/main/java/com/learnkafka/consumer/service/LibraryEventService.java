package com.learnkafka.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.consumer.repository.LibraryEventRepo;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    private LibraryEventRepo libraryEventRepo;

    @Autowired
    private ObjectMapper objectMapper;

    public void processLibEvent(ConsumerRecord<Integer, String> data) throws JsonProcessingException {
        log.info( "processLibEvent data {}",data);
        LibraryEvent LibraryEvent=objectMapper.readValue(data.value(),LibraryEvent.class);
        // only for testing recovery code
        if(null != LibraryEvent.getLibrarayEventID() && LibraryEvent.getLibrarayEventID()==999){
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }
        switch (LibraryEvent.getLibraryEventType()){
            case NEW:
                save(LibraryEvent);
                break;
            case UPDATE:
                update(LibraryEvent);
                break;
            default:
                log.info("Invalid Library Event Type");
        }
    }

    private void save(LibraryEvent libraryEvent) {
        log.info("save method started libraryEvent {} ",libraryEvent);
        libraryEvent.getBookDetails().setLibraryEvent(libraryEvent);
        libraryEventRepo.save(libraryEvent);
        log.info("save method ended ");
    }

    private void update(LibraryEvent libraryEvent) {
        log.info("update method started libraryEvent {}",libraryEvent);
        if(null == libraryEvent.getLibrarayEventID()){
            throw new IllegalArgumentException("Library Event ID is missing");
        }
        Optional<LibraryEvent> libraryEventOptional= libraryEventRepo.findById(libraryEvent.getLibrarayEventID());
        if(!libraryEventOptional.isPresent())
            throw new IllegalArgumentException("Not valid L=library Event");
        save(libraryEvent);
        log.info("update method ended ");
    }
}
