package com.github.dkirrane.kproducer.producer;

import com.github.javafaker.Faker;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
@Slf4j
public class GenericProducer {

    public static String SASL_JAAS_TEMPLATE = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";

    @Value( "${kafka.serviceUri}" )
    private String serviceUri;

    @Value( "${kafka.username}" )
    private String username;

    @Value( "${kafka.password}" )
    private String password;

    @Value( "${kafka.schemaRegistryUri}" )
    private String schemaRegistryUri;

    private Properties createProducerConfig() throws Exception {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serviceUri);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(SASL_JAAS_TEMPLATE, username, password));
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        /* https://docs.confluent.io/current/schema-registry/serializer-formatter.html */
//        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
//        props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, config.getKafka().getAutoRegisterSchemas());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }

    @Bean
    public KafkaProducer<String, String> createProducer() throws Exception {
        return new KafkaProducer<>(createProducerConfig());
    }

    @Bean
    public Faker createFaker() {
        return new Faker();
    }
}
