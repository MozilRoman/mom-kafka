package com.mom.kafka.controller;

import com.mom.kafka.service.ProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class Controller {
    private static final Logger LOGGER = LoggerFactory.getLogger(Controller.class);
    private ProducerService producerService;

    @Autowired
    public Controller(ProducerService producerService) {
        this.producerService = producerService;
    }

    @GetMapping("/produce")
    public ResponseEntity<String> produceToQueue() {
        try {
            LOGGER.info("preparing msg");
            producerService.send();
            return new ResponseEntity<>("Produced ADD messages successfully", HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>("ADD messages did not delivered", HttpStatus.BAD_REQUEST);
        }
    }

    @GetMapping("/produceTransactional")
    public ResponseEntity<String> produceTransactional() {
        try {
            LOGGER.info("preparing msg");
            producerService.sendMessageWithTransaction();
            return new ResponseEntity<>("Produced ADD messages successfully", HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>("ADD messages did not delivered", HttpStatus.BAD_REQUEST);
        }
    }
}
