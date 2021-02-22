package com.clgx.tax.poc.clip.config;
import com.google.auto.service.AutoService;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        System.out.println("JVM initialization action before pipeline execution"); // this shows up in the worker log
         String localTrustoreLocation = "/tmp/client.truststore.jks";
        String localKeyTabLocation = "/tmp/client-svc.keytab";
         String krb5ConfigFileLocation = "/tmp/krb5.conf";

/*
gs://gcdf_dev_configs/kafka/dev-config/dev-client-config
gs://gcdf_dev_configs/kafka/dev-config/dev-client.truststore.jks
gs://gcdf_dev_configs/kafka/dev-config/dev-krb5.conf
gs://gcdf_dev_configs/kafka/dev-config/dev-tax-dpl-svc.keytab

 */
        Storage storage = StorageOptions.getDefaultInstance().getService();
       // URI uri = URI.create(options.getKeyTab());
        URI uri = URI.create("gs://gcdf_dev_configs/kafka/dev-config/dev-tax-dpl-svc.keytab");
        System.out.println(String
                .format("Keytab file URI: %s, filesystem: %s, bucket: %s, filename: %s", uri.toString(),
                        uri.getScheme(), uri.getAuthority(),
                        uri.getPath()));
        Blob keytabBlob = storage.get(BlobId.of(uri.getAuthority(),
                uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath()));
        Path localKeytabPath = Paths.get(localKeyTabLocation);
        System.out.println(localKeytabPath.toString());

        keytabBlob.downloadTo(localKeytabPath);

        uri = URI.create("gs://gcdf_dev_configs/kafka/dev-config/dev-client.truststore.jks");
        System.out.println(String
                .format("TrustStore file URI: %s, filesystem: %s, bucket: %s, filename: %s", uri.toString(),
                        uri.getScheme(), uri.getAuthority(),
                        uri.getPath()));
        Blob trustStoreBlob = storage.get(BlobId.of(uri.getAuthority(),
                uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath()));
        Path trustStorePath = Paths.get(localTrustoreLocation);
        System.out.println(trustStorePath.toString());
        trustStoreBlob.downloadTo(trustStorePath);

        uri = URI.create("gs://gcdf_dev_configs/kafka/dev-config/dev-krb5.conf");
        System.out.println(String
                .format("Krb5.conf file URI: %s, filesystem: %s, bucket: %s, filename: %s", uri.toString(),
                        uri.getScheme(), uri.getAuthority(),
                        uri.getPath()));
        Blob krb5ConfBlob = storage.get(BlobId.of(uri.getAuthority(),
                uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath()));
        Path krb5ConfigFilePath = Paths.get(krb5ConfigFileLocation);
        System.out.println(krb5ConfigFilePath.toString());
        krb5ConfBlob.downloadTo(krb5ConfigFilePath);

        System.setProperty("java.security.krb5.conf", krb5ConfigFileLocation);


            System.out.println("JVM initialization action before pipeline execution");
        }

}
