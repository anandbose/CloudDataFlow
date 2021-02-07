package com.clgx.tax.pas.poc.bq.pipeline;


import com.clgx.tax.pas.poc.bq.model.input.*;
import com.clgx.tax.pas.poc.bq.model.output.CombinedRecordsets;
import com.clgx.tax.pas.poc.bq.model.output.Installment;
import com.clgx.tax.pas.poc.bq.model.output.Parcel;
import com.clgx.tax.pas.poc.bq.model.output.bqSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BQReadWrite implements Serializable {
    private  static String projectId;
    private   static String dataSet;
    private  static String tableName;
    private  static Logger log = LoggerFactory.getLogger(BQReadWrite.class);
    public static PCollection<KV<String, bqSchema>> readBQdata(String project, String dataset, String table, Pipeline p1, String partitiondate)
    {
        projectId = project;
        dataSet = dataset;
        tableName = table;
        PCollection<KV<String,bqSchema>> YesterdaysBQData = p1.apply("ReadYesterdaysdata-fromBQ-PasBills",
                BigQueryIO.readTableRows()

                        //.from(getTableReference())
                        .fromQuery("select * from `"+projectId+"."+dataSet+"."+tableName+"` where GOOD_THRU_DT >=  '"+partitiondate+"'")
                        .usingStandardSql()
                )

                .apply("ConverttoPCollection",
                        MapElements.into(TypeDescriptor.of(bqSchema.class)).via(bqTestPasSchema::convertToObj)
                )
                .apply("add key",ParDo.of(new DoFn<bqSchema, KV<String,bqSchema>>(){
                    @ProcessElement
                   // @DefaultCoder(Avi)
                    public void processElement(@Element bqSchema Input , OutputReceiver<KV<String,bqSchema>> out)
                    {
                        log.info("The data is::"+Input.toString());

                        KV<String,bqSchema> op= KV.of(Input.getParcelKey(),Input);
                        out.output(op);
                    }
                }));
        return YesterdaysBQData;
    }

    public static PCollection<KV<String, CombinedRecordsets>> generateNewRecords(String project, String dataset, String table, PCollection<KV<String, CoGbkResult>> collection,
                                                                                 TupleTag<PasPrcl> pasPrcltuple,
                                                                                 TupleTag<PasPrclOwn>  pasprclowntuple,
                                                                                 TupleTag<PasLiens> lientuple ,
                                                                                 TupleTag<PasBills> billtuple ,
                                                                                 TupleTag<PasBillsInst> insttuple ,
                                                                                 TupleTag<PasBillAmt> amttuple,
                                                                                 TupleTag<bqSchema> bqTuple)
    {
        PCollection<KV<String,CombinedRecordsets>> differentialRecords =
                collection.apply("Create the differential records", ParDo.of(new DoFn<KV<String, CoGbkResult>,KV<String,CombinedRecordsets>>(){
                    @ProcessElement
                    public void processElement(@Element  KV<String,CoGbkResult> input, OutputReceiver<KV<String,CombinedRecordsets>> out)
                    {
                        Iterable<PasPrcl> PasPrclrecs = input.getValue().getAll(pasPrcltuple);
                        Iterable<PasPrclOwn> PasPrcslOwnrecs = input.getValue().getAll(pasprclowntuple);
                        Iterable<PasLiens> PasLienrecs = input.getValue().getAll(lientuple);
                        Iterable<PasBills> PasBillrecs = input.getValue().getAll(billtuple);
                        Iterable<PasBillsInst> PasBillInstrecs = input.getValue().getAll(insttuple);
                        Iterable<PasBillAmt> PasAmtrecs = input.getValue().getAll(amttuple);
                        Iterable<bqSchema> BqPasRecs = input.getValue().getAll(bqTuple);
                      //  bqSchema bqprocess = new bqSchema();
                        List<KV<String,CombinedRecordsets> > combRecs = bqTestPasSchema.convertToBqSchema(
                                PasPrclrecs ,
                                PasPrcslOwnrecs ,
                                PasLienrecs ,
                                PasBillrecs ,
                                PasBillInstrecs ,
                                PasAmtrecs
                        );
                        boolean newRec = false;
                        for(KV<String,CombinedRecordsets> combRec : combRecs)
                        {
                            List<Installment> insList = new ArrayList<>();

                            for (Installment installment: combRec.getValue().getPrcl().getInstallments())
                            {
                                Installment ins = new Installment();
                                ins = installment;

                                List<bqSchema> newRecords = new ArrayList<>();


                                for(bqSchema bqRecord: installment.getBigQueryRecs())
                                {
                                    boolean match = false;

                                        for(bqSchema existingRec: BqPasRecs) {
                                            if (bqRecord.getHashKey().equals(existingRec.getHashKey()))
                                                match = true;
                                        }
                                        if (!match) {

                                            /**Create Parcel Record and installment records**/
                                            log.info("hash key didnot match so definitely a new bqRecord");
                                            newRecords.add(bqRecord);

                                        }
                                }
                                if (newRecords.size() > 0)
                                {
                                    log.info("Add these new records to the installment::"+ installment.toString());
                                    ins.setBigQueryRecs(newRecords);
                                    newRec = true;
                                    insList.add(ins);

                                }



                            }
                            if (newRec)
                            {
                                //add installment to parcel
                                CombinedRecordsets recset = new CombinedRecordsets();
                                Parcel prcl = combRec.getValue().getPrcl();
                                prcl.setInstallments(insList);
                                recset.setPrcl(prcl);
                                out.output(KV.of(prcl.getPrclKey(),recset));
                            }
                        }
                    }
                }));
            return differentialRecords;
    }



    public  static  void writetoBQ(
            String project, String dataset, String table, PCollection<KV<String,CombinedRecordsets>> collection,

            Long Days)
    {
        projectId = project;
        dataSet = dataset;
        tableName = table;

       // getTableReference();
        //get the table rows
        PCollection<TableRow> bqrows =
                collection.apply("Create the BQ Rows", ParDo.of(new DoFn<KV<String, CombinedRecordsets>,TableRow>(){
                    @ProcessElement
                    public void processElement(@Element  KV<String,CombinedRecordsets> input, OutputReceiver<TableRow> out)
                    {

                        bqTestPasSchema bqprocess = new bqTestPasSchema();
                        List<TableRow> rows = bqprocess.convertToRow(
                           input.getValue() ,Days
                        );
                        for(TableRow row : rows)
                        {
                            out.output(row);
                        }
                    }
                }));

        TimePartitioning partition = new TimePartitioning().setType("DAY").setField("");
        //write the rows
        bqrows.apply("Write to bigquery",BigQueryIO.writeTableRows()
        .to(getTableReference())
              //  .withSchema()
                .withJsonSchema(bqTestPasSchema.getJsonTableSchema())
      //  .withSchema(bqTestPasSchema.getPasSchema())
                .withTimePartitioning(new TimePartitioning().setType("DAY").setField("GOOD_THRU_DT").setRequirePartitionFilter(true))
      //  .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND))
        ;

    }

    public static  TableReference getTableReference()
    {
        TableReference tableSpec =
                new TableReference()
                        .setProjectId(projectId)
                        .setDatasetId(dataSet)
                        .setTableId(tableName)
                ;

        return tableSpec;
    }
}
