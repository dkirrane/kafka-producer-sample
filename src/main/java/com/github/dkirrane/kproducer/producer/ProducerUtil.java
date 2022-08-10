package com.github.dkirrane.kproducer.producer;

import com.github.javafaker.Faker;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Component
public class ProducerUtil {

    @Value( "${app.inputTopic}" )
    private String inputTopic;

    @Autowired
    private KafkaProducer<String, String> producer;

    @Autowired
    private Faker faker;

    private final List<String> keys = IntStream.range(1, 100)
            .mapToObj(value -> String.valueOf(value))
            .collect(Collectors.toList());

    @Scheduled(fixedDelay = 500)
    public void check() throws Exception {

        final String topic = inputTopic;
        final String key = keys.get(faker.random().nextInt(keys.size()));
        final String value = faker.chuckNorris().fact();

        log.info("INPUT>>> topic: {} \t key: {} \t value: {}", topic, key, value);

        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record);
    }
}

