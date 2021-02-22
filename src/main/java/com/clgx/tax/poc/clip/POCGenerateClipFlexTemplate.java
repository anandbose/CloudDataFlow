package com.clgx.tax.poc.clip;

import com.clgx.tax.poc.clip.config.FlexClipPipelineOptions;
import com.clgx.tax.poc.clip.mappers.MaptoPasPrcl;
import com.clgx.tax.poc.clip.model.PasPrcl;
import com.clgx.tax.poc.clip.model.avro.User;
import com.clgx.tax.poc.clip.pipeline.HttpWriter;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class POCGenerateClipFlexTemplate {


    static Logger log = LoggerFactory.getLogger(POCGenerateClipFlexTemplate.class);

    public static class MyClassKafkaAvroDeserializer extends AbstractKafkaAvroDeserializer implements Deserializer<User> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            configure(new KafkaAvroDeserializerConfig(configs));
        }

        @Override
        public User deserialize(String s, byte[] bytes) {
            return (User) this.deserialize(bytes);
        }
    }

    public static class MyClassKafkaAvroSerializer extends AbstractKafkaAvroSerializer implements Serializer<User> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            configure(new KafkaAvroSerializerConfig(configs));
        }

        @Override
        public byte[] serialize(String s, User myData) {
            return super.serializeImpl(s, myData);
        }

    }

    public static void main(String[] args) {
        FlexClipPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(FlexClipPipelineOptions.class);
        runPasPipeline(options);

    }


    public   static void runPasPipeline(FlexClipPipelineOptions options)
    {

        Pipeline p1 = Pipeline.create(options);
        String delimiter="\\|";
/**
 * Set kerberos config and kafka config
 */
        Map<String, Object> commonKafkaConfig = new HashMap<>();
       // Map<String,Object> consumerConfig = new HashMap<>();

        if(true) {
            String saslJaasConfig = "com.sun.security.auth.module.Krb5LoginModule required " +
                    "useKeyTab=true " +
                    "storeKey=true " +
                    "keyTab=\"/tmp/client-svc.keytab\" " +
                    "principal=\"dev-tax-dpl-svc@IDAP.CORELOGIC.COM\";";
            //commonKafkaConfig.put("security.protocol", "SASL_SSL");
            commonKafkaConfig.put("sasl.mechanism","GSSAPI");
            commonKafkaConfig.put("ssl.truststore.location", "/tmp/client.truststore.jks");
            commonKafkaConfig.put("sasl.jaas.config", saslJaasConfig);
            commonKafkaConfig.put("ssl.truststore.password", "r1ufh9jKB5ieRVFHuQQxyYU6P");
            commonKafkaConfig.put("sasl.kerberos.service.name", "kafka");
            commonKafkaConfig.put("auto.offset.reset", "latest");
            commonKafkaConfig.put("schema.registry.url", options.getKfkSchemRegistry());
            commonKafkaConfig.put("specific.avro.reader", true);
            commonKafkaConfig.put("group.id", "test-avro");
            //set consumer congig
           // consumerConfig.put("")

        }
        /**
         * Read from kafka
         *
         *
         */

       PCollection<User> userObj = p1.apply("Read from Kafka", KafkaIO.<String, User>read()
       .withBootstrapServers(options.getKfkBrokerServer())
               .withTopic(options.getKfkTriggerTopic())
               .withConsumerConfigUpdates(commonKafkaConfig)
               .withKeyDeserializer(StringDeserializer.class)
               .withValueDeserializer(MyClassKafkaAvroDeserializer.class)
               .withReadCommitted()
                       .commitOffsetsInFinalize()
                       .withoutMetadata()

       ).apply ("Process the message",ParDo.of(new DoFn<KV<String, User>, User>() {
           @ProcessElement
           public void processData(@Element KV<String,User> input,OutputReceiver<User> Out)
           {
               log.info("The element is :::County::"+input.getValue().getCounty()+"::State::"+input.getValue().getState());
               Out.output(input.getValue());
           }


       }))

       ;
        /**
         *
         * Read the PAS Parcels and store data in pcollection
         *
         */

        String pasPrclPrefix = "PAS_PRCL_STCN";
        log.info("The HTTP url is "+options.getHttpUrl());
        String HttpUrl = options.getHttpUrl();
        String apiKey = options.getApiKey();

        PCollection<KV<String, PasPrcl>> parcels = p1.apply("Read PAS Parcels", TextIO.read().from(
                ValueProvider.NestedValueProvider.of(ValueProvider.StaticValueProvider.of(options.getFilePrefix()),  new SerializableFunction<String, String>()
                {
                    @Override
                    public String apply(String input) {
                        String[] fields = input.split("-");
                        String dt = fields[2];

                        return fields[0]+pasPrclPrefix+fields[1]+"_"+fields[2];
                    }
                })
                )
         ).apply("convert to parcel object", ParDo.of(
                new DoFn<String, KV<String, PasPrcl>>() {
                    @ProcessElement
                    public void processElement(@Element String Input, OutputReceiver<KV<String, PasPrcl>> out) {
                        String[] fields = Input.split(delimiter);

                        PasPrcl obj = new MaptoPasPrcl().maptoprcl(fields,HttpUrl,apiKey);
                        KV<String,PasPrcl> kvObj = KV.of(obj.getPRCL_KEY(),obj);
                        log.info("Current time is::"+Instant.now());
                        out.outputWithTimestamp(kvObj,Instant.now());
                    }
                }
        )).apply("Filter only TXA records",Filter.by((SerializableFunction<KV<String, PasPrcl>, Boolean>) input -> {
            PasPrcl prcl = input.getValue();

            if (prcl.getSOR_CD().equals("TXA"))
                return true;
            else
                return false;
        }));


        /**
         * Clip the parcel data
         * Creat window as well
         */

        Duration windowDuration = Duration.standardMinutes(1);
        Window<KV<String, PasPrcl>> window =
                Window.<KV<String, PasPrcl>>into(FixedWindows.of(windowDuration))
                        //Window.<KV<String, PasPrcl>>into(new GlobalWindows())
                                                   .triggering(Never.ever())
                                                    .accumulatingFiredPanes()
                                                    .withAllowedLateness(Duration.standardSeconds(10));
        PCollection<KV<String, PasPrcl>> clippedParcels = parcels.apply(window).apply("Clip the parcels",new HttpWriter<>());


        //Convert clipped to a parcel collection



        /*write clipped data to file*/

        clippedParcels.apply("write to file afterflatteing" , MapElements.via(new SimpleFunction<KV<String, PasPrcl>, String>() {
                    @Override
                    public String apply(KV<String, PasPrcl> input) {
                        return (input.getValue().createOutput());
                    }
                })

        )
              //  .apply("writeTofile",TextIO.write().withoutSharding().to(ValueProvider.StaticValueProvider.of("/Users/anbose/MyApplications/SparkPOCFiles/PAS/lacounty/input/inputs/small/Clipinfo.txt")));
                .apply("writeTofile",TextIO.write().withoutSharding().to(ValueProvider.StaticValueProvider.of(options.getOutputFileName()+"-"+"PAS_PARCEL_CLIPPED")));

        /**
         * Read the PAS Parcel owner and store data in pcollection
         */


        /**
         * Run the pipeline

         */
        p1.run();
    }
}
