package com.clgx.tax.pas.poc.bq.config;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

public interface FlexPipelineOptions extends PipelineOptions {

        @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/lacounty/input/inputs/small/-04019-20201216")
        String getFilePrefix();
        void setFilePrefix(String fileName);

        String getFileName();
        void setFileName(String fileName);

        @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/lacounty/input/inputs/small/output-0409-20201216")
        String getOutputFileName();
        void setOutputFileName(String fileName);

        @Default.String("elasticuser")
        String getElasticUsername();
        void setElasticUsername(String username);

        @Default.String("elasticpwd")
        String getElasticPwd();
        void setElasticPwd(String Pwd);

    @Default.String("http://localhost:9200")
    String getElasticUrl();
    void setElasticUrl(String url);
    @Default.Long(0)
    Long getDays();
    void  setDays(Long days);

    @Default.String("01-01-2021")
    String getPartitionDate();
    void setPartitionDate(String partitionDate);

    // static String elasticUrl = "https://cc317dd9125743c9a2f563cf4d48dd06.int-ece-main-green-proxy.elastic.int.idap.clgxdata.com:9243";
  //  static String userName = "clgx_service";
  //  static String elasticPassword = "clgx_service_r0ck$";
}
