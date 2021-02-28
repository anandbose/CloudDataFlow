package com.clgx.tax.poc.clip.services;

import com.clgx.tax.poc.clip.model.avro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerAvroConfig {


    public static void main(final String[] args) {

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "dev-kafka-blue-broker-1.kafka.dev.cloud.clgxdata.com:9093");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "https://dev-kafka-blue-registry-1.kafka.dev.cloud.clgxdata.com:8081");

        if(true) {
            String saslJaasConfig = "com.sun.security.auth.module.Krb5LoginModule required " +
                    "useKeyTab=true " +
                    "storeKey=true " +
                    "debug=true "+
                    "keyTab=\"/tmp/client-svc.keytab\" " +
                    "principal=\"dev-tax-dpl-svc@IDAP.CORELOGIC.COM\";";

            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism","GSSAPI");
            props.put("ssl.truststore.location", "/tmp/client.truststore.jks");
            props.put("sasl.jaas.config", saslJaasConfig);
            props.put("ssl.truststore.password", "r1ufh9jKB5ieRVFHuQQxyYU6P");
            props.put("sasl.kerberos.service.name", "kafka");


        }

        try (KafkaProducer<String, User> producer = new KafkaProducer<String, User>(props))
        {



            User user =new User("04","019","20201216");
            final ProducerRecord<String, User> record = new ProducerRecord<String, User>("dtp.poc.pasdata.trigger", user.getDate().toString(), user);
            producer.send(record);
         //   Thread.sleep(1000L);


            producer.flush();
            System.out.printf("Successfully produced 1 message to a topic called %s%n", "dtp.poc.pasdata.trigger");

        } catch (final SerializationException e) {
            e.printStackTrace();
        }

    }
}
