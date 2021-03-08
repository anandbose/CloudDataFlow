
package com.clgx.tax.pas.poc.bq;

import com.clgx.tax.pas.poc.bq.config.FlexPipelineOptions;
import com.clgx.tax.pas.poc.bq.mappers.MaptoPasPrcl;
import com.clgx.tax.pas.poc.bq.mappers.MaptoPrclOwn;
import com.clgx.tax.pas.poc.bq.model.input.*;
import com.clgx.tax.pas.poc.bq.pipeline.BQReadWrite;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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

     /*   PCollection<KV<String, bqSchema>> YesterdaysBQData = BQReadWrite.readBQdata(
                "clgx-dtetl-spark-dev-fc0e","exploratory","pas_data_temp",p1,options.getPartitionDate()
        );*/


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
        /**
         * Step 2 Join the tables
         */

        PCollection<KV<String, CoGbkResult>> groupedCollection = KeyedPCollectionTuple
                .of(pasPrcltuple,parcels)
                .and(pasprclowntuple,parcelOwners)
                .and(lientuple,parcelLiens)
                .and(billtuple,parcelBills)
                .and(insttuple,parcelBillInstallments)
                .and(amttuple,parcelBillAmounts)
                .apply("Join-Data-Via-PRCLkey", CoGroupByKey.create());





        /*Write to BQ*/
        PCollection<TableRow> bqrows =
                groupedCollection.apply("Create the BQ Rows", ParDo.of(new DoFn<KV<String, CoGbkResult>,TableRow>(){
                    @ProcessElement
                    public void processElement(@Element  KV<String,CoGbkResult> input, OutputReceiver<TableRow> out)
                    {

                        Iterable<PasPrcl> PasPrclrecs = input.getValue().getAll(pasPrcltuple);
                        Iterable<PasPrclOwn> PasPrcslOwnrecs = input.getValue().getAll(pasprclowntuple);
                        Iterable<PasLiens> PasLienrecs = input.getValue().getAll(lientuple);
                        Iterable<PasBills> PasBillrecs = input.getValue().getAll(billtuple);
                        Iterable<PasBillsInst> PasBillInstrecs = input.getValue().getAll(insttuple);
                        Iterable<PasBillAmt> PasAmtrecs = input.getValue().getAll(amttuple);
                        //  Iterable<bqSchema> BqPasRecs = input.getValue().getAll(bqTuple);

                        TableRow mainrow;
                        List<TableRow> rows = new ArrayList<TableRow>();
                        for (PasPrcl prcl: PasPrclrecs)
                        {
                            mainrow = new TableRow();
                            mainrow.set("HASHKEYVAL", prcl.getClipNumber());
                            mainrow.set("PARCELKEY",prcl.getPRCL_KEY());
                            mainrow.set("CLIPNUMBER", prcl.getClipNumber());
                            mainrow.set("SOR_CD",prcl.getSOR_CD());
                            //Setting Address
                            List<TableRow> Addresses = new ArrayList<>();
                            List<TableRow> Owners = new ArrayList<>();
                            List<TableRow> Liens = new ArrayList<>();
                            BQReadWrite createBQRecs = new BQReadWrite();

                            Addresses.add(createBQRecs.getAddressMapping(prcl));

                            for (PasPrclOwn owner: PasPrcslOwnrecs)
                            {
                                Owners.add(createBQRecs.getOwmerMapping(owner));

                            }
                            for (PasLiens lien : PasLienrecs)
                            {
                                Liens.add(createBQRecs.getLienMapping(lien,PasBillrecs,PasBillInstrecs,PasAmtrecs));
                            }
                            mainrow.set("OWNERS",Owners);
                            mainrow.set("ADDRESS",Addresses);
                            mainrow.set("LIENS",Liens);
                            rows.add(mainrow);

                        }
                        for(TableRow individualrow : rows)
                        {
                            out.output(individualrow);
                        }
                    }
                }));

        //write the rows
        bqrows. apply("Filter only TXA" , Filter.by(new SerializableFunction<TableRow, Boolean>() {
            @Override
            public Boolean apply(TableRow input) {

                return ((String)input.get("SOR_CD")).equals("TXA") ? Boolean.TRUE:Boolean.FALSE;
            }
        }))
                .  apply("Write to bigquery", BigQueryIO.writeTableRows()
                        .to(new BQReadWrite("clgx-dtetl-spark-dev-fc0e","exploratory","pas_nested_table_04019")
                                .getTableReference())
                        //  .withSchema()
                        .withJsonSchema(new BQReadWrite().getJsonTableSchema())
                        //  .withSchema(bqTestPasSchema.getPasSchema())
                        //      .withTimePartitioning(new TimePartitioning().setType("DAY").setField("GOOD_THRU_DT").setRequirePartitionFilter(true))
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))
        ;
        //convert the differential records to Json string and write the records



        p1.run();
    }
}
