package com.clgx.tax.poc.clip.config;
import com.google.auto.service.AutoService;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Security;

@AutoService(JvmInitializer.class)
public class MyJvmInitializer implements JvmInitializer{
   Logger log = LoggerFactory.getLogger(MyJvmInitializer.class);
    @Override
    public void onStartup() {
       log.info("initializing JVM to diable ssl");
        System.out.println("Initializing new property for security");
        Security.setProperty("jdk.tls.disabledAlgorithms", "");
    }

    @Override
    public void beforeProcessing(PipelineOptions options) {

            System.out.println("JVM initialization action before pipeline execution");
        }

}
