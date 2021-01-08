
package com.clgx.tax.beam.pipelines.samples;

import com.clgx.tax.beam.pipelines.poc.sdp.HttpWriter;
import com.clgx.tax.beam.pipelines.poc.sdp.HttpWriterAsynch;
import com.clgx.tax.beam.pipelines.poc.sdp.HttpWriterPlain;
import com.clgx.tax.data.model.poc.*;
import com.clgx.tax.data.model.poc.output.*;
import com.clgx.tax.mappers.poc.MaptoPasPrcl;
import com.clgx.tax.mappers.poc.MaptoPrclOwn;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class POCGenerateClip {


    static Logger log = LoggerFactory.getLogger(POCGenerateClip.class);
    public static void main(String[] args) {
        pasPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(pasPipelineOptions.class);
        runPasPipeline(options);

    }

    public interface pasPipelineOptions extends PipelineOptions {


      //  @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/-02003-20201207")
      @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/lacounty/input/inputs/small/-04019-20201216")
        ValueProvider<String> getFilePrefix();
        void setFilePrefix(ValueProvider<String> fileName);

        ValueProvider<String> getFileName();
        void setFileName(ValueProvider<String> fileName);

      //  @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/out-02003-20201207")
      @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/lacounty/input/inputs/small/output-0409-20201216")
        ValueProvider<String> getOutputFileName();
        void setOutputFileName(ValueProvider<String> fileName);

        @Default.String("http://localhost:9200")
        ValueProvider<String> getElasticUrl();
        void setElasticUrl(ValueProvider<String> url);
    }

    public   static void runPasPipeline(pasPipelineOptions options)
    {

        Pipeline p1 = Pipeline.create(options);
        String delimiter="\\|";
        /**
         * Read the PAS Parcels and store data in pcollection
         */

        String pasPrclPrefix = "PAS_PRCL_STCN";

        PCollection<KV<String, PasPrcl>> parcels = p1.apply("Read PAS Parcels", TextIO.read().from(
                ValueProvider.NestedValueProvider.of(options.getFilePrefix(),  new SerializableFunction<String, String>()
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
                        PasPrcl obj = new MaptoPasPrcl().maptoprcl(fields);
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
                .apply("writeTofile",TextIO.write().withoutSharding().to(ValueProvider.NestedValueProvider.of(options.getOutputFileName(),
                        new SerializableFunction<String,String>(){

                            @Override
                            public String apply(String input) {
                                return input+"-"+"PAS_PARCEL_CLIPPED";
                            }
                        })));

        /**
         * Read the PAS Parcel owner and store data in pcollection
         */


        /**
         * Run the pipeline

         */
        p1.run();
    }
}
