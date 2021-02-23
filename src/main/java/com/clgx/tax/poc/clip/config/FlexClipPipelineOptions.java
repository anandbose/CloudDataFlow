package com.clgx.tax.poc.clip.config;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface FlexClipPipelineOptions extends PipelineOptions {
    //  @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/-02003-20201207")
    @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/lacounty/input/inputs/small/-04019-20201216")
    @Validation.Required
    String getFilePrefix();
    void setFilePrefix(String fileName);

    String getFileName();
    void setFileName(String fileName);

    //  @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/out-02003-20201207")
    @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/lacounty/input/inputs/small/output-04019-20201216")
    String getOutputFileName();
    void setOutputFileName(String fileName);

    @Default.String("http://localhost:9200")
    String getHttpUrl();
    void setHttpUrl(String url);

    @Default.String("apikey")
    String getApiKey();
    void setApiKey(String apiKey);

    /////Set Kerberos files

    @Default.String("gs://clgx-scdf-data-dev-xwei/kafka/secure/int-app-prcl-ply-svc.keytab")
    String getKeyTab();
    void setKeyTab(String keyTab);

/////Kafka topics
    @Default.String("dev-kafka-blue-broker-1.kafka.dev.cloud.clgxdata.com:9093")
    String getKfkBrokerServer();
    void  setKfkBrokerServer(String kfkBrokerServer);

    @Default.String("dtp.poc.pasdata.trigger")
    String getKfkTriggerTopic();
    void  setKfkTriggerTopic(String kfkTriggerTopic);

    @Default.String("https://dev-kafka-blue-registry-1.kafka.dev.cloud.clgxdata.com:8081")
    String getKfkSchemRegistry();
    void  setKfkSchemRegistry(String kfkSchemRegistry);

}
