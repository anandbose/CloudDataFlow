package com.clgx.tax.poc.clip.services;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import com.clgx.tax.poc.clip.model.avro.User;

import java.util.Properties;

public class ProducerAvroConfig {
    private static final String TOPIC = "dtp.poc.pasdata.trigger";

    public static void main(final String[] args) {
     //   System.setProperty("java.security.krb5.conf","/Users/anbose/Documents/A2020/kafka/dev-krb5.conf");
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
                    "debug=true " +
                    "keyTab=\"/Users/anbose/Documents/A2020/kafka/dev-tax-dpl-svc.keytab\" " +
                    "principal=\"dev-tax-dpl-svc@IDAP.CORELOGIC.COM\";";
            props.put("sasl.mechanism","GSSAPI");
            props.put("security.protocol", "SASL_SSL");
            props.put("ssl.truststore.location", "/Users/anbose/Documents/A2020/kafka/dev-client.truststore.jks");
            props.put("sasl.jaas.config", saslJaasConfig);
            props.put("ssl.truststore.password", "r1ufh9jKB5ieRVFHuQQxyYU6P");
            props.put("sasl.kerberos.service.name", "kafka");
        }

        try (KafkaProducer<String, User> producer = new KafkaProducer<String, User>(props)) {

            for (long i = 0; i < 1; i++) {
                final String orderId = "id" + i + "-" + System.currentTimeMillis();
                final User User = new User("04","019","20201216");
                final String key = User.getCounty().toString()+"-"+User.getState().toString();
                final ProducerRecord<String, User> record = new ProducerRecord<String, User>(TOPIC, key, User);
                producer.send(record);
                Thread.sleep(1000L);
            }

            producer.flush();
            System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);

        } catch (final SerializationException e) {
            e.printStackTrace();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }

    }

}
