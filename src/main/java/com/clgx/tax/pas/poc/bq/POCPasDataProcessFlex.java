
package com.clgx.tax.pas.poc.bq;

import com.clgx.tax.pas.poc.bq.config.FlexPipelineOptions;
import com.clgx.tax.pas.poc.bq.mappers.MaptoPasPrcl;
import com.clgx.tax.pas.poc.bq.mappers.MaptoPrclOwn;
import com.clgx.tax.pas.poc.bq.model.input.*;
import com.clgx.tax.pas.poc.bq.pipeline.BQReadWrite;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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


        //Read the revisoin rable
        String projectId = "clgx-dtetl-spark-dev-fc0e";
        String dataSet = "exploratory";
        String table = "pas_nested_table_04019";


        PCollection<KV<String,TableRow>> revisionRows = p1.apply("Read the revision",BigQueryIO.readTableRows()
                //   .from(new BQReadWrite("clgx-dtetl-spark-dev-fc0e","exploratory","pas_nested_table_04019_revision")
                // .getTableReference())
                .fromQuery("select CURRENTREVISION from `"+projectId+"."+dataSet+"."+table+"_revision`")
                .usingStandardSql())
                .apply("create kv",MapElements.via(new SimpleFunction<TableRow, KV<String, TableRow>>() {
                    @Override
                    public KV<String, TableRow> apply(TableRow input) {
                        return KV.of("test",input);
                    }
                }));

// create new revision

        PCollection<TableRow> outputRevision = revisionRows.apply("create output revision",MapElements.via(new SimpleFunction<KV<String,TableRow>,TableRow>()
        {
            @Override
            public TableRow apply(KV<String,TableRow> input) {
                String currRev = (String)input.getValue().get("CURRENTREVISION");
                TableRow row = new TableRow();
                row.set( "CHANGEDTIME",LocalDateTime.now().format(DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSS")));
                if (currRev.equals("R1"))
                {
                    row.set("CURRENTREVISION","R2");
                }
                else{
                    row.set("CURRENTREVISION","R1");

                }
                return row;
            }
        }));



        /*Write to BQ*/
        PCollection<KV<String,TableRow>> bqrows =
                groupedCollection.apply("Create the BQ Rows", ParDo.of(new DoFn<KV<String, CoGbkResult>,KV<String,TableRow>>(){
                    @ProcessElement
                    public void processElement(@Element  KV<String,CoGbkResult> input, OutputReceiver<KV<String,TableRow>> out)
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
                            out.output(KV.of("test",individualrow));
                        }
                    }
                }));

//combine bq rows and revision

        final TupleTag<TableRow> bqRowTuple = new TupleTag<>();
        final TupleTag<TableRow>  revisionTuple = new TupleTag<>();
        PCollection<KV<String, CoGbkResult>> trCollection = KeyedPCollectionTuple
                .of(bqRowTuple,bqrows)
                .and(revisionTuple,revisionRows)
                .apply("Join-Data-Via-Statickey", CoGroupByKey.create());

        //Iterate and create multiple collections

        PCollection<TableRow> bqRowsR1=trCollection.apply("Prallely process for R1",ParDo.of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
            @ProcessElement
            public void processElement(@Element  KV<String,CoGbkResult> input, OutputReceiver<TableRow> out)
            {
                Iterable<TableRow> bqRows = input.getValue().getAll(bqRowTuple);
                Iterable<TableRow> revisions = input.getValue().getAll(revisionTuple);
                for (TableRow bqRow: bqRows)
                    for (TableRow revision: revisions)
                    {
                        String rev =(String) revision.get("CURRENTREVISION");
                        if (rev.equals("R1"))
                        {
                            out.output(bqRow);
                        }
                    }
            }

        }));

        PCollection<TableRow> bqRowsR2=trCollection.apply("Prallely process for R1",ParDo.of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
            @ProcessElement
            public void processElement(@Element  KV<String,CoGbkResult> input, OutputReceiver<TableRow> out)
            {
                Iterable<TableRow> bqRows = input.getValue().getAll(bqRowTuple);
                Iterable<TableRow> revisions = input.getValue().getAll(revisionTuple);
                for (TableRow bqRow: bqRows)
                    for (TableRow revision: revisions)
                    {
                        String rev =(String) revision.get("CURRENTREVISION");
                        if (rev.equals("R2"))
                        {
                            out.output(bqRow);
                        }
                    }
            }

        }));

        PCollection<TableRow> bqRows=trCollection.apply("Prallely process for R1",ParDo.of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
            @ProcessElement
            public void processElement(@Element  KV<String,CoGbkResult> input, OutputReceiver<TableRow> out)
            {
                Iterable<TableRow> bqRows = input.getValue().getAll(bqRowTuple);
                Iterable<TableRow> revisions = input.getValue().getAll(revisionTuple);
                for (TableRow bqRow: bqRows)
                    for (TableRow revision: revisions)
                    {
                        String rev =(String) revision.get("CURRENTREVISION");
                        TableRow newRow = new TableRow();
                        newRow = bqRow.clone();

                        newRow.set("CURRENTREVISION",rev);
                        try {
                            log.info("new Data is ::"+newRow.toPrettyString());

                        }
                        catch(Exception ex)
                        {
                            log.error("Error adding current Revision::"+ex.getMessage());
                        }
                            out.output(newRow);

                    }
            }

        }));

     /*   bqRowsR1. apply("Filter only TXA" , Filter.by(new SerializableFunction<TableRow , Boolean>() {

            @Override
            public Boolean apply(TableRow input) {

                return ((String)(input.get("SOR_CD"))).equals("TXA") ? Boolean.TRUE:Boolean.FALSE;
            }


        }))
                .  apply("Write to bigquery", BigQueryIO.writeTableRows()
                        .to(new BQReadWrite("clgx-dtetl-spark-dev-fc0e","exploratory","pas_nested_table_04019_R2")
                                .getTableReference())
                        .withJsonSchema(new BQReadWrite().getJsonTableSchema())

                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))
        ;
        //convert the differential records to Json string and write the records
        bqRowsR2. apply("Filter only TXA" , Filter.by(new SerializableFunction<TableRow , Boolean>() {

            @Override
            public Boolean apply(TableRow input) {

                return ((String)(input.get("SOR_CD"))).equals("TXA") ? Boolean.TRUE:Boolean.FALSE;
            }


        }))
                .  apply("Write to bigquery", BigQueryIO.writeTableRows()
                        .to(new BQReadWrite("clgx-dtetl-spark-dev-fc0e","exploratory","pas_nested_table_04019_R1")
                                .getTableReference())
                        .withJsonSchema(new BQReadWrite().getJsonTableSchema())

                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))

        ;*/

        //write dynamically for each row

        bqRows.apply(BigQueryIO.<TableRow>write()
        .to(new DynamicDestinations<TableRow, String>() {


            @Override
            public String getDestination(@UnknownKeyFor @NonNull @Initialized ValueInSingleWindow<TableRow> element) {
                return (String)element.getValue().get("CURRENTREVISION");
            }
            @Override
            public TableDestination getTable(String revision) {
                String strSuffix = "R1";
                if(revision.equals("R1"))
                {
                    strSuffix = new String("R2");
                }

                return new TableDestination(
                        new TableReference()
                                .setProjectId("clgx-dtetl-spark-dev-fc0e")
                        .setDatasetId("exploratory")
                        .setTableId("pas_nested_table_04019_"+strSuffix),"Revision::"+strSuffix

                );
            }

            @Override
            public @UnknownKeyFor @NonNull @Initialized TableSchema getSchema(String destination) {
                return new TableSchema()
                        .setFields(
                                ImmutableList.of(
                                        new TableFieldSchema()
                                                .setName("HASHKEYVAL")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("PARCELKEY")
                                                .setType("STRING")
                                                .setMode("REQUIRED"),
                                        new TableFieldSchema()
                                                .setName("SOR_CD")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("CLIPNUMBER")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("GEO_SRC_CD")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("FIPS_STATE_ID")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("FIPS_CNTY_ID")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("SRVY_ID")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("BLDG_CLS_CD")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("LAND_TYP_CD")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("LAND_USE_NM")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("PRPTY_SOLD_DT")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("LAST_IMPRV_YR")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("OWN_ACQ_DT")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("PRPTY_ZONE_CD")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("CENSUS_TRACT_CD")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("FLOOD_ZONE_CD")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("NO_TXAUTH_FLG")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("ORIG_SRC_CD")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("MOBL_HOME_VIN_ID")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("ASSESS_PRCL_ID")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("FILE_CRTE_DT")
                                                .setType("DATETIME")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("NON_ADV_PRPTY_DESC")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("NON_ADV_PRPTY_VALUE_AMT")
                                                .setType("FLOAT64")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("MAN_RSRCH_CD")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("UAT_ADDR_TAG_ID")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("PRCL_USE_NM")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("PRCL_SCND_USE_CD")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("FULL_EXMPT_FLG")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("TAX_AREA")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("LAND_TYP_NM")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("BLDG_CLS_NM")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("NGHBRHD_CD")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("GEO_CD_LATTD")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("GEO_CD_LONGTD")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("MAP_GRID_LOC_TXT")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("UNSCRB_SITUS_ADDR_TXT")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("UNSCRB_SITUS_ADDR_LN2_TXT")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("CNTY_ID")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("STATE_ID")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("CURRENTREVISION")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("ADDRESS")
                                                .setType("RECORD")
                                                .setMode("REPEATED")
                                                .setFields(

                                                        ImmutableList.of(
                                                                new TableFieldSchema()
                                                                        .setName("STRT_PRE_DIR_CD")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("STRT_NBR_TXT")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("STRT_NBR_END_TXT")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("STRT_NBR_FRCTN_TXT")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("STRT_NM")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("UNIT_TYP")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("UNIT_NBR_TXT")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("UNIT_NBR_END_TXT")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("PO_BOX_NBR_TXT")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("BOX_LOC_TXT")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("RR_CD")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("RR_NBR")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("NGHBRHD_NM")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("CITY_NM")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("CNTY_NM")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("POSTAL_CD")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("CNTRY_CD")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("STATE_CD")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("STATE_SUB_CD")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"))),
                                        new TableFieldSchema()
                                                .setName("OWNERS")
                                                .setType("RECORD")
                                                .setMode("REPEATED")
                                                .setFields(
                                                        ImmutableList.of(
                                                                new TableFieldSchema()
                                                                        .setName("OWN_TYP")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("CMPNY_NM")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("LAST_NM")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("MTRNL_LAST_NM")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("FRST_NM")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("MID_NM")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("OWN_PFX_NM")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("OWN_SFX_NM")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE")
                                                        )
                                                ),


                                        new TableFieldSchema()
                                                .setName("LIENS")
                                                .setType("RECORD")
                                                .setMode("REPEATED")
                                                .setFields(
                                                        ImmutableList.of(

                                                                new TableFieldSchema()
                                                                        .setName("LIEN_KEY")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("TAX_ID")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("TXAUTH_ID")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("TXAUTH_FILE_TYP")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("LIEN_TYP")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("DSPLY_TAX_ID_FLG")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("MUNI_CD")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("SCHL_DSTRC_CD")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("PEND_APRTN_FLG")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("PEND_APRTN_DT")
                                                                        .setType("DATETIME")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("ORIG_SRC_CD")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("LGCY_PRIM_TXAUTH_ID")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("LGCY_PRIM_TAX_ID")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("PREV_TAX_ID")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("PRIOR_YR_DELQ_FLG")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("MAN_RSRCH_CD")
                                                                        .setType("STRING")
                                                                        .setMode("NULLABLE"),
                                                                new TableFieldSchema()
                                                                        .setName("BILLS")
                                                                        .setType("RECORD")
                                                                        .setMode("REPEATED")
                                                                        .setFields(
                                                                                ImmutableList.of(

                                                                                        new TableFieldSchema()
                                                                                                .setName("BILL_TYP")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("TAX_BILL_BGN_YR")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("TAX_BILL_END_YR")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("TXAUTH_TAX_FOR_BGN_YR")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("TXAUTH_TAX_FOR_END_YR")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("TXAUTH_BILL_TAX_ID")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("PYMT_PLAN_ID")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("PYMT_PLAN_CD")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("PYMT_PLAN_STAT_CD")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("ORIG_SRC_CD")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("BILL_CD")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("BILL_STAT_CD")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("BILL_UID")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("MAN_RSRCH_CD")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("PYMT_PLAN_DFLT_DT")
                                                                                                .setType("DATETIME")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("PYMT_PLAN_LAST_PAY_DT")
                                                                                                .setType("DATETIME")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("PYMT_PLAN_UNPD_BAL_AMT")
                                                                                                .setType("FLOAT64")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("PYMT_STAT_CD")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                         new TableFieldSchema()
                                                                                                .setName("BILL_EXMPT_FLG")
                                                                                                .setType("STRING")
                                                                                                .setMode("NULLABLE"),
                                                                                        new TableFieldSchema()
                                                                                                .setName("INSTALLMENTS")
                                                                                                .setType("RECORD")
                                                                                                .setMode("REPEATED")
                                                                                                .setFields(
                                                                                                        ImmutableList.of(


                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("INSTL_CD")
                                                                                                                        .setType("STRING")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("TAX_BILL_ID")
                                                                                                                        .setType("STRING")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("TAX_PYMT_SRC_CD")
                                                                                                                        .setType("STRING")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("DELQ_DT")
                                                                                                                        .setType("DATETIME")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("PD_DT")
                                                                                                                        .setType("DATETIME")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("TXAUTH_POST_DT")
                                                                                                                        .setType("DATETIME")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("MAIL_DT")
                                                                                                                        .setType("DATETIME")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("TXAUTH_TAX_SRCH_DT")
                                                                                                                        .setType("DATETIME")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("TXAUTH_FILE_TYP")
                                                                                                                        .setType("STRING")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("ORIG_SRC_CD")
                                                                                                                        .setType("STRING")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("FILE_CRTE_DT")
                                                                                                                        .setType("DATETIME")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("FILE_AQRD_DT")
                                                                                                                        .setType("DATETIME")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("MAN_RSRCH_CD")
                                                                                                                        .setType("STRING")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("DELAY_BILL_CD")
                                                                                                                        .setType("STRING")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("TXAUTH_ZERO_AMT_CD")
                                                                                                                        .setType("STRING")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("RDMPTN_SPCL_DOC_CD")
                                                                                                                        .setType("STRING")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("PAYEE_ID")
                                                                                                                        .setType("STRING")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("STAT_CD")
                                                                                                                        .setType("STRING")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("BILL_EXMPT_FLG")
                                                                                                                        .setType("STRING")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("NSF_FLG")
                                                                                                                        .setType("STRING")
                                                                                                                        .setMode("NULLABLE"),
                                                                                                                new TableFieldSchema()
                                                                                                                        .setName("AMOUNT")
                                                                                                                        .setType("RECORD")
                                                                                                                        .setMode("REPEATED")
                                                                                                                        .setFields(
                                                                                                                                ImmutableList.of(
                                                                                                                                        new TableFieldSchema()
                                                                                                                                                .setName("BILL_AMT_TYP")
                                                                                                                                                .setType("STRING")
                                                                                                                                                .setMode("NULLABLE"),
                                                                                                                                        new TableFieldSchema()
                                                                                                                                                .setName("CRNCY_TYP")
                                                                                                                                                .setType("STRING")
                                                                                                                                                .setMode("NULLABLE"),
                                                                                                                                        new TableFieldSchema()
                                                                                                                                                .setName("BILL_AMT")
                                                                                                                                                .setType("FLOAT64")
                                                                                                                                                .setMode("NULLABLE"),
                                                                                                                                        new TableFieldSchema()
                                                                                                                                                .setName("GOOD_THRU_DT")
                                                                                                                                                .setType("DATETIME")
                                                                                                                                                .setMode("NULLABLE")
                                                                                                                                )
                                                                                                                        )

                                                                                                        )
                                                                                                )




                                                                                )
                                                                        )


                                                        )
                                                )


                                )






                        );
            }

        }).withFormatFunction((TableRow row)->row)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));


        outputRevision.apply("write the revision table",BigQueryIO.writeTableRows()
                .to(new BQReadWrite("clgx-dtetl-spark-dev-fc0e","exploratory","pas_nested_table_04019_revision")
                        .getTableReference())
                .withJsonSchema(new BQReadWrite("clgx-dtetl-spark-dev-fc0e","exploratory","pas_nested_table_04019_revision")
                        .getJsonTableSchema())

                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        p1.run();
    }
}
