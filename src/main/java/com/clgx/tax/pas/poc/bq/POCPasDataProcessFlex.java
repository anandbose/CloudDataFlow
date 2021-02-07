
package com.clgx.tax.pas.poc.bq;

import com.clgx.tax.pas.poc.bq.model.output.CombinedRecordsets;
import com.clgx.tax.pas.poc.bq.model.output.bqSchema;
import com.clgx.tax.pas.poc.bq.pipeline.BQReadWrite;
import com.clgx.tax.pas.poc.bq.config.FlexPipelineOptions;
import com.clgx.tax.pas.poc.bq.model.input.*;
import com.clgx.tax.pas.poc.bq.model.output.Parcel;
import com.clgx.tax.pas.poc.bq.mappers.MaptoPasPrcl;
import com.clgx.tax.pas.poc.bq.mappers.MaptoPrclOwn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class POCPasDataProcessFlex {

   // static String elasticUrl = "http://10.128.15.219:9200";


    static Logger log = LoggerFactory.getLogger(POCPasDataProcessFlex.class);


    /**Main program where the pipeline options and the pipeline initialized**/

    public static void main(String[] args) {
        FlexPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(FlexPipelineOptions.class);
      //  options.as(DataflowPipelineDebugOptions.class).setNumberOfWorkerHarnessThreads(2);

        runPasPipeline(options);

    }



    public   static void runPasPipeline(FlexPipelineOptions options)
    {
        /***
         * Add the elastic search credentials
         * **/
        String elasticUrl = options.getElasticUrl();
         String userName = options.getElasticUsername();
         String elasticPassword = options.getElasticPwd();
        Pipeline p1 = Pipeline.create(options);
        String delimiter="\\|";
        /**
         * Read the PAS Parcels (Clipped) and store data in pcollection
         */

        String pasPrclPrefix = "PAS_PARCEL_CLIPPED";

        PCollection<KV<String, PasPrcl>> parcels = p1.apply("Read PAS Parcels", TextIO.read().from(
                ValueProvider.StaticValueProvider.of(options.getOutputFileName()+"-"+pasPrclPrefix)
                )
         ).apply("convert to parcel object", ParDo.of(
                new DoFn<String, KV<String, PasPrcl>>() {
                    @ProcessElement
                    public void processElement(@Element String Input, OutputReceiver<KV<String, PasPrcl>> out) {
                        String[] fields = Input.split(delimiter);
                        PasPrcl obj = new MaptoPasPrcl().maptoprcl(fields);
                        KV<String,PasPrcl> kvObj = KV.of(obj.getPRCL_KEY(),obj);
                        out.output(kvObj);
                    }
                }
        ));


       /**
         * Read the PAS Parcel owner and store data in pcollection
         */


        String pasPrclOwnPrefix = "PAS_PRCL_OWN_STCN";
        String[] fields = options.getFilePrefix().split("-");
        PCollection<KV<String, PasPrclOwn>> parcelOwners = p1.apply("Read PAS Parcel Owner", TextIO.read().from(
                ValueProvider.StaticValueProvider.of(fields[0]+pasPrclOwnPrefix+fields[1]+"_"+fields[2]))
        ).apply("convert to parcel owner object", ParDo.of(
                new DoFn<String, KV<String, PasPrclOwn>>() {
                    @ProcessElement
                    public void processElement(@Element String Input, OutputReceiver<KV<String, PasPrclOwn>> out) {
                        String[] fields = Input.split(delimiter);
                        PasPrclOwn obj = new MaptoPrclOwn().maptoprcl(fields);
                        KV<String,PasPrclOwn> kvObj = KV.of(obj.getPRCL_KEY(),obj);
                        out.output(kvObj);
                    }
                }
        ));

        /**
         * Read the PAS Parcel Liens and store data in pcollection
         */
        String pasPrclLiensPrefix = "PAS_PRCL_LIENS_STCN";

        PCollection<KV<String, PasLiens>> parcelLiens = p1.apply("Read PAS Parcel Liens", TextIO.read().from(
                ValueProvider.StaticValueProvider.of(fields[0]+pasPrclLiensPrefix+fields[1]+"_"+fields[2]))

        ).apply("convert to parcel Lien object", ParDo.of(
                new DoFn<String, KV<String, PasLiens>>() {
                    @ProcessElement
                    public void processElement(@Element String Input, OutputReceiver<KV<String, PasLiens>> out) {
                        String[] fields = Input.split(delimiter);
                        PasLiens obj = new MaptoPasPrcl().mapToLiens(fields);
                        KV<String,PasLiens> kvObj = KV.of(obj.getPRCL_KEY(),obj);
                        out.output(kvObj);
                    }
                }
        ));

        /**
         * Read the PAS Bills  and store data in pcollection
         */
        String pasPrclBillsPrefix = "PAS_PRCL_BILLS_STCN";
        PCollection<KV<String, PasBills>> parcelBills = p1.apply("Read PAS Parcel Bills ", TextIO.read().from(
                ValueProvider.StaticValueProvider.of(fields[0]+pasPrclBillsPrefix+fields[1]+"_"+fields[2]))
        ).apply("convert to parcel Bills object", ParDo.of(
                new DoFn<String, KV<String, PasBills>>() {
                    @ProcessElement
                    public void processElement(@Element String Input, OutputReceiver<KV<String, PasBills>> out) {
                        String[] fields = Input.split(delimiter);
                        PasBills obj = new MaptoPasPrcl().mapToBills(fields);
                        KV<String,PasBills> kvObj = KV.of(obj.getPRCL_KEY(),obj);
                        out.output(kvObj);
                    }
                }
        ));

        /**
         * Read the PAS Bills Inst and store data in pcollection
         */
        String pasPrclBillsInstPrefix = "PAS_PRCL_BILL_INSTLM_STCN";
        PCollection<KV<String, PasBillsInst>> parcelBillInstallments = p1.apply("Read PAS Parcel Bills Installments", TextIO.read().from(
                ValueProvider.StaticValueProvider.of(fields[0]+pasPrclBillsInstPrefix+fields[1]+"_"+fields[2]))

        ).apply("convert to parcel Bill Install object", ParDo.of(
                new DoFn<String, KV<String, PasBillsInst>>() {
                    @ProcessElement
                    public void processElement(@Element String Input, OutputReceiver<KV<String, PasBillsInst>> out) {
                        String[] fields = Input.split(delimiter);
                        PasBillsInst obj = new MaptoPasPrcl().mapToBillsInst(fields);
                        KV<String,PasBillsInst> kvObj = KV.of(obj.getPRCL_KEY(),obj);
                        out.output(kvObj);
                    }
                }
        ));



        /**
         * Read the PAS Bills Amt and store data in pcollection
         */
        String pasPrclBillsAmtPrefix = "PAS_PRCL_BILL_AMT_STCN";
        PCollection<KV<String, PasBillAmt>> parcelBillAmounts = p1.apply("Read PAS Parcel Bill Amounts", TextIO.read().from(
                ValueProvider.StaticValueProvider.of(fields[0]+pasPrclBillsAmtPrefix+fields[1]+"_"+fields[2]))

        ).apply("convert to parcel Bill Amount object", ParDo.of(
                new DoFn<String, KV<String, PasBillAmt>>() {
                    @ProcessElement
                    public void processElement(@Element String Input, OutputReceiver<KV<String, PasBillAmt>> out) {
                        String[] fields = Input.split(delimiter);
                        PasBillAmt obj = new MaptoPasPrcl().mapToBillAmt(fields);
                   //     log.info("Parcel key is::"+obj.getPRCL_KEY());
                        KV<String,PasBillAmt> kvObj = KV.of(obj.getPRCL_KEY(),obj);
                        out.output(kvObj);
                    }
                }
        ));

        /***
         *
         * ReadwriteBq , read from the big query to check previous hash keys
         *
         */

        PCollection<KV<String, bqSchema>> YesterdaysBQData = BQReadWrite.readBQdata(
                "clgx-dtetl-spark-dev-fc0e","exploratory","pas_data_temp",p1,options.getPartitionDate()
        );


        /**
         * Join all the tables using the parcelkey
         *
         * Step 1:: Create the tuples
         */
        //join the records...
        final TupleTag<PasPrcl> pasPrcltuple = new TupleTag<>();
        final TupleTag<PasPrclOwn>  pasprclowntuple = new TupleTag<>();
        final TupleTag<PasLiens> lientuple = new TupleTag<>();
        final TupleTag<PasBills> billtuple = new TupleTag<>();
        final TupleTag<PasBillsInst> insttuple = new TupleTag<>();
        final TupleTag<PasBillAmt> amttuple = new TupleTag<>();
        final TupleTag<bqSchema> bqSchemaType = new TupleTag<>();
        /**
         * Step 2 Join the tables
         */

        PCollection<KV<String, CoGbkResult>> groupedCollection = KeyedPCollectionTuple
                .of(pasPrcltuple,parcels)
                .and(pasprclowntuple,parcelOwners)
                .and(bqSchemaType,YesterdaysBQData)
                .and(lientuple,parcelLiens)
                .and(billtuple,parcelBills)
                .and(insttuple,parcelBillInstallments)
                .and(amttuple,parcelBillAmounts)
                .apply("Join-Data-Via-PRCLkey", CoGroupByKey.create());

        /**
         *
         * Identify the differential records
         *
         */

        PCollection<KV<String,CombinedRecordsets>> differentialRecords = BQReadWrite.generateNewRecords("clgx-dtetl-spark-dev-fc0e","exploratory","pas_data_temp",groupedCollection,
                pasPrcltuple,
                pasprclowntuple ,
                lientuple,
                billtuple ,
                insttuple,
                amttuple,
                bqSchemaType
        );

        /**
         * Take the differential records and create the parcel object and installment object
         *
         */


        /*Write to BQ*/
        BQReadWrite.writetoBQ("clgx-dtetl-spark-dev-fc0e","exploratory","pas_data_temp",differentialRecords,
         options.getDays() );
        //convert the differential records to Json string and write the records

        differentialRecords.apply("Map to parcel",MapElements
                .into(TypeDescriptor.of(Parcel.class))
                .via((KV<String, CombinedRecordsets> kvRec) ->  kvRec.getValue().getPrcl())
        ).apply("ConvertToJson" ,  AsJsons.of(Parcel.class))
        .apply("Write to File",TextIO.write().withoutSharding().to(ValueProvider.StaticValueProvider.of(options.getOutputFileName()+"-elastic.txt")))
        ;

        p1.run();
    }
}
