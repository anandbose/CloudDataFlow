package com.clgx.tax.poc.clip.config;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface FlexClipPipelineOptions extends PipelineOptions {
    //  @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/-02003-20201207")
    @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/lacounty/input/inputs/small/-04019-20201216")
    @Validation.Required
    String getFilePrefix();
    void setFilePrefix(String fileName);

    String getFileName();
    void setFileName(String fileName);

    //  @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/out-02003-20201207")
    @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/lacounty/input/inputs/small/output-0409-20201216")
    String getOutputFileName();
    void setOutputFileName(String fileName);

    @Default.String("http://localhost:9200")
    String getHttpUrl();
    void setHttpUrl(String url);

    @Default.String("apikey")
    String getApiKey();
    void setApiKey(String apiKey);


}
