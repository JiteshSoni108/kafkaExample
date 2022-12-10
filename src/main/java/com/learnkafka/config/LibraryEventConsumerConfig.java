package com.learnkafka.config;

import com.learnkafka.consumer.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;
import java.util.Map;

@Component
@EnableKafka
@Slf4j
public class LibraryEventConsumerConfig {

    public static final String RETRY="RETRY";
    public static final String DEAD="DEAD";
    public static final String SUCCESS="SUCCESS";

    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private FailureService failureService;

    @Value("${topics.retry:library-events.RETRY}")
    private String retryTopic;

    @Value("${topics.dlt:library-events.DLT}")
    private String deadLetterTopic;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, (ConsumerFactory) kafkaConsumerFactory.getIfAvailable(() -> {
            return new DefaultKafkaConsumerFactory(this.kafkaProperties.buildConsumerProperties());
        }));
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL); // default is batch
        factory.setCommonErrorHandler(errorHandler());
        factory.setConcurrency(3); // not need for cloud application
        return factory;
    }

    public DefaultErrorHandler errorHandler() {
        //retry after every 1 second
        var ignoreExceptionsList = List.of(IllegalArgumentException.class);
        var retryExceptionsList = List.of(RecoverableDataAccessException.class);
        var fixedBackOff = new FixedBackOff(1000L, 2);

        //This is for custom handler
        //var errorHandler = new DefaultErrorHandler(fixedBackOff);
        //retry for specific exceptions not for all

        var errorHandler = new DefaultErrorHandler(
                publishingRecoverer(),
                //consumerRecordRecoverer // This function method will save error data in DB
                fixedBackOff//expBackOff
        );
        ignoreExceptionsList.forEach(errorHandler::addNotRetryableExceptions); // this will ignore the exception for retry
        retryExceptionsList.forEach(errorHandler::addRetryableExceptions); // this retry for given exception
        //helpful for debug purpose
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.info("Failed retry exception :{} , attempts {} , record {} ", ex.getMessage(), deliveryAttempt, record);
        });

        //We can set backoff for retry
        /*var expBackOff= new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1_000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2_000L);
        var errorHandler= new DefaultErrorHandler(expBackOff);*/
        return errorHandler;
    }

    //This method will push to retry topic, if the exception is of type RecoverableDataAccessException.class
    public DeadLetterPublishingRecoverer publishingRecoverer() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (r, e) -> {
            log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
            if (e.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(retryTopic, r.partition());
            } else {
                return new TopicPartition(deadLetterTopic, r.partition());
            }
        });
        return recoverer;
    }

    public ConsumerRecordRecoverer consumerRecordRecoverer = (record, exception) -> {
        log.error("Exception is : {} Failed Record : {} ", exception, record);
        if (exception.getCause() instanceof RecoverableDataAccessException) {
            log.info("Inside the recoverable logic");
            //Add any Recovery Code here.
            failureService.saveFailedRecord((ConsumerRecord<Integer, String>) record, exception, RETRY);

        } else {
            log.info("Inside the non recoverable logic and skipping the record : {}", record);
            failureService.saveFailedRecord((ConsumerRecord<Integer, String>) record, exception, DEAD);
        }
    };
}
