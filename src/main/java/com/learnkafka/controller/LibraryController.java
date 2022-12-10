package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producor.LibraryEventProducor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
@RequestMapping("/libraryEvent")
public class LibraryController {

    @Autowired
    private LibraryEventProducor libraryEventProducor;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("start postLibraryEvent libraryEvent {}",libraryEvent);
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducor.sendLibraryEvent(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent/update")
    public ResponseEntity<?> updateLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("start updateLibraryEvent libraryEvent {}",libraryEvent);
        if(libraryEvent.getLibrarayEventID() ==null){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the Event ID");
        }
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEventProducor.sendLibraryEvent(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
