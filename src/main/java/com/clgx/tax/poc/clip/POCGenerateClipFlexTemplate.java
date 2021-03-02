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
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
        String   fileNameprefix = options.getFilePrefix();
        if(true) {
            String saslJaasConfig = "com.sun.security.auth.module.Krb5LoginModule required " +
                    "useKeyTab=true " +
                    "storeKey=true " +
                    "debug=true "+
                    "keyTab=\"/tmp/client-svc.keytab\" " +
                    "principal=\"dev-tax-dpl-svc@IDAP.CORELOGIC.COM\";";
            commonKafkaConfig.put("security.protocol", "SASL_SSL");
            commonKafkaConfig.put("sasl.mechanism","GSSAPI");
            commonKafkaConfig.put("ssl.truststore.location", "/tmp/client.truststore.jks");
            commonKafkaConfig.put("sasl.jaas.config", saslJaasConfig);
            commonKafkaConfig.put("ssl.truststore.password", "r1ufh9jKB5ieRVFHuQQxyYU6P");
            commonKafkaConfig.put("sasl.kerberos.service.name", "kafka");
            commonKafkaConfig.put("auto.offset.reset", "latest");
            commonKafkaConfig.put("schema.registry.url", options.getKfkSchemRegistry());
            commonKafkaConfig.put("specific.avro.reader", true);
            commonKafkaConfig.put("group.id", "cloudflow-1");
            commonKafkaConfig.put("client.id","dataflow-client-poc-2");
            commonKafkaConfig.put("enable.auto.commit","true");

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
           //    .withMaxNumRecords(1)
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

     //   userObj.ma
      //  PCollection<KV<String, String>> parcels =
        PCollection<KV<String, PasPrcl>> parcels =
                userObj.apply("get file Namee for pas parcel",ParDo.of(new DoFn<User,String>()
                                                                       {
                                                                           //String fileNameprefix="";

                                                                           @ProcessElement
                                                                           public void  apply(@Element User obj ,OutputReceiver<String> out)
                                                                           {

                                                                               out.output( fileNameprefix+pasPrclPrefix+obj.getCounty()+obj.getState()+"_"+obj.getDate());

                                                                           }

            }
    )
        )
                .apply(FileIO.matchAll())
                .apply("Read PAS Parcels",FileIO.readMatches())
                .apply("Read the TextFiles",TextIO.readFiles())
        .apply("convert to parcel object", ParDo.of(
                new DoFn<String, KV<String, PasPrcl>>() {
                    @ProcessElement
                    public void processElement(@Element String Input, OutputReceiver<KV<String, PasPrcl>> out) {
                        String[] fields = Input.split(delimiter);

                        PasPrcl obj = new MaptoPasPrcl().maptoprcl(fields,HttpUrl,apiKey);
                        KV<String,PasPrcl> kvObj = KV.of(obj.getPRCL_KEY(),obj);
                        log.info("Current time is::"+Instant.now());
                        out.output(kvObj);
                    }
                }
        )).apply("Filter only TXA records", Filter.by((SerializableFunction<KV<String, PasPrcl>, Boolean>) input -> {
            PasPrcl prcl = input.getValue();

            return prcl.getSOR_CD().equals("TXA");
        }));;




        /**
         * Clip the parcel data
         * */

        PCollection<KV<String, PasPrcl>> clippedParcels = parcels.apply("Clip the parcels",new HttpWriter<>());


        //Convert clipped to a parcel collection



        /*write clipped data to file*/

        clippedParcels.apply("write to file afterflatteing" , MapElements.via(new SimpleFunction<KV<String, PasPrcl>, String>() {
                    @Override
                    public String apply(KV<String, PasPrcl> input) {
                        return (input.getValue().createOutput());
                    }
                })

        );
              //  .apply("writeTofile",TextIO.write().withoutSharding().to(ValueProvider.StaticValueProvider.of("/Users/anbose/MyApplications/SparkPOCFiles/PAS/lacounty/input/inputs/small/Clipinfo.txt")));
          //      .apply("writeTofile",TextIO.write().withoutSharding().to(ValueProvider.StaticValueProvider.of(options.getOutputFileName()+"-"+"PAS_PARCEL_CLIPPED")));

        /**
         * Read the PAS Parcel owner and store data in pcollection
         */


        /**
         * Run the pipeline

         */
        p1.run();
    }
}
