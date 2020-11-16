package com.clgx.tax.beam.pipelines.samples;

import com.clgx.tax.data.model.BQPasBills;
import com.clgx.tax.data.model.pas.PasBillAmt;
import com.clgx.tax.data.model.pas.PasBills;
import com.clgx.tax.data.model.pas.PasBillsInst;
import com.clgx.tax.data.model.pas.PasLiens;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PasDataJoinMultipleDataSets {
    static Logger log = LoggerFactory.getLogger(PasDataJoinMultipleDataSets.class);
    public static void main(String[] args) {
       PasBillsOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PasBillsOptions.class);

        runPasPipeline(options);
    }

    public interface PasBillsOptions extends PipelineOptions {
        //Ideltify the four PAS Input files
        @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/MaricopaCounty/Unzipped/testfiles/PAS_PRCL_LIENS_copy.csv")
        ValueProvider<String> getPasLiens();
        void setPasLiens(ValueProvider<String> value);

        @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/MaricopaCounty/Unzipped/testfiles/PAS_PRCL_BILLS_copy.csv")
        ValueProvider<String> getPasBills();
        void setPasBills(ValueProvider<String> value);

        @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/MaricopaCounty/Unzipped/testfiles/PAS_PRCL_BILL_INSTL_copy.csv")
        ValueProvider<String> getPasBillsInst();
        void setPasBillsInst(ValueProvider<String> value);

        @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/MaricopaCounty/Unzipped/testfiles/PAS_PRCL_BILL_AMT_copy.csv")
        ValueProvider<String> getPasBillsAmt();
        void setPasBillsAmt(ValueProvider<String> value);


        //Output files
        @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/MaricopaCounty/Unzipped/testfiles/output_pas_data.csv")
        ValueProvider<String> getOutputPasData();
        void setOutputPasData(ValueProvider<String> value);


        @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/MaricopaCounty/Unzipped/testfiles/output_agg_data")
        ValueProvider<String> getOutputPasAggegation();
        void setOutputPasAggegation(ValueProvider<String> value);


    }

    static void runPasPipeline(PasBillsOptions options)
    {
        Pipeline p1 = Pipeline.create(options);


        //Pas Bills Amount

        PCollection<KV<String, PasBillAmt>> Amt =p1.apply("Read PAS Bills Amount",TextIO.read().from(options.getPasBillsAmt()))
                .apply("PCollection - PasBillsInst",ParDo.of(new DoFn<String, KV<String, PasBillAmt> >() {
                    @ProcessElement
                    public void processElement(@Element String Input, OutputReceiver<KV<String, PasBillAmt>> out)
                    {
                        String[] field = Input.split(",");
                        PasBillAmt obj = new PasBillAmt();
                        obj.setPRCL_KEY(field[0]);
                        obj.setBILL_KEY(field[3]);
                        obj.setLIEN_KEY(field[2]);
                        obj.setSOR_CD(field[1]);
                        obj.setSTATE_ID(field[22]);
                        obj.setCNTY_ID(field[21]);
                        obj.setSTAT_CD(field[13]);
                        obj.setPRCL_BILL_INSTL_KEY(field[4]);
                        if (field[9]!=null && !field[9].trim().equals(""))
                            obj.setBILL_AMT(Double.valueOf(field[9]));
                        obj.setBILL_AMT_TYP(field[7]);
                        obj.setGOOD_THRU_DT(field[10]);
                      //  obj.setINSTL_CD(field[8]);
                        KV<String,PasBillAmt> op = KV.of(obj.getPRCL_KEY(),obj);
                        out.output(op);
                    }
                }));

        //Pas Bills Inst

        PCollection<KV<String, PasBillsInst>> Inst =p1.apply("Read PAS Bills Inst",TextIO.read().from(options.getPasBillsInst()))
                .apply("PCollection - PasBillsInst",ParDo.of(new DoFn<String, KV<String, PasBillsInst> >() {
                    @ProcessElement
                    public void processElement(@Element String Input, OutputReceiver<KV<String, PasBillsInst>> out)
                    {
                        String[] field = Input.split(",");
                        PasBillsInst obj = new PasBillsInst();
                        obj.setPRCL_KEY(field[0]);
                        obj.setBILL_KEY(field[3]);
                        obj.setLIEN_KEY(field[2]);
                        obj.setSOR_CD(field[1]);
                        obj.setSTATE_ID(field[35]);
                        obj.setCNTY_ID(field[34]);
                        obj.setSTAT_CD(field[26]);
                        obj.setPRCL_BILL_INSTL_KEY(field[4]);
                        obj.setINSTL_CD(field[8]);
                        KV<String,PasBillsInst> op = KV.of(obj.getPRCL_KEY(),obj);
                        out.output(op);
                    }
                }));

        //Pas Liens
        PCollection<KV<String, PasLiens>> liens = p1.apply("Read PASLiens",TextIO.read().from(options.getPasLiens()))
                .apply("PCollection - PasLiens",ParDo.of(new DoFn<String, KV<String, PasLiens> >() {
                    @ProcessElement
                    public void processElement(@Element String Input, OutputReceiver<KV<String, PasLiens>> out)
                    {
                        String[] field = Input.split(",");
                        PasLiens obj = new PasLiens();
                        obj.setPRCL_KEY(field[0]);
                        obj.setLIEN_KEY(field[2]);
                        obj.setSOR_CD(field[1]);
                        obj.setSTATE_ID(field[6]);
                        obj.setCNTY_ID(field[7]);
                        obj.setTXAUTH_ID(field[4]);
                        obj.setTAX_ID(field[3]);
                        KV<String , PasLiens> op = KV.of(obj.getPRCL_KEY(),obj);
                        out.output(op);
                    }
                }));
        //Pas Bills
        PCollection<KV<String, PasBills>> bills = p1.apply("Read PASBBILLS" , TextIO.read().from(options.getPasBills()))
                .apply("PCollection - PasBills", ParDo.of(new DoFn<String, KV<String, PasBills>>() {

                    // @DefaultCoder(Avi)
                    @ProcessElement
                    public void processElement(@Element String Input, OutputReceiver<KV<String, PasBills>> out) {
                        String[] field = Input.split(",");

                        PasBills obj = new PasBills();
                        obj.setPRCL_KEY(field[0]);
                        obj.setSOR_CD(field[1]);
                        obj.setLIEN_KEY(field[2]);
                        obj.setBILL_KEY(field[3]);
                        obj.setLAST_UPDT_TS(field[29]);
                        obj.setSTATE_ID(field[33]);
                        obj.setCNTY_ID(field[32]);
                        obj.setTAX_BILL_BGN_YR(field[7]);
                        obj.setTAX_BILL_END_YR(field[8]);
                        obj.setSTAT_CD(field[24]);
                        obj.setBILL_TYP(field[6]);

                        KV<String, PasBills> kvObj = KV.of(obj.getPRCL_KEY(), obj);

                        out.output(kvObj);

                        //  if (field.length < 10) return;)
                    }

                }));

        //join the records...
        final TupleTag<PasLiens> lientuple = new TupleTag<>();
        final TupleTag<PasBills> billtuple = new TupleTag<>();
        final TupleTag<PasBillsInst> insttuple = new TupleTag<>();
        final TupleTag<PasBillAmt> amttuple = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> groupedCollection = KeyedPCollectionTuple
                .of(lientuple,liens)
                .and(billtuple,bills)
                .and(insttuple,Inst)
                .and(amttuple,Amt)
                .apply("Join-Data-Via-PRCLkey", CoGroupByKey.create());

        //** get all amount records based on the join

        PCollection<PasBillAmt> output = groupedCollection.apply( "Join the records and filter::",
                ParDo.of(new DoFn<KV<String, CoGbkResult>, PasBillAmt>() {
                    @ProcessElement
                    public void processElement(@Element KV<String,CoGbkResult> input, OutputReceiver<PasBillAmt> out){
                        Iterable<PasLiens> lienrecs = input.getValue().getAll(lientuple);
                        Iterable<PasBills> billrecs = input.getValue().getAll(billtuple);
                        Iterable<PasBillsInst> instrecs = input.getValue().getAll(insttuple);
                        Iterable<PasBillAmt> amtrecs = input.getValue().getAll(amttuple);
                        //**** Iterate and do the joins
                        for (PasLiens lienrec : lienrecs)
                        {
                            log.info("Executing for Lien::"+lienrec.getLIEN_KEY());
                            if (lienrec.getTXAUTH_ID().equals("020070000") && lienrec.getSOR_CD().equals("TXA")) {
                                for (PasBills billrec : billrecs) {
                                    log.info("Executing for Bill::"+billrec.getBILL_KEY());

                                    if(
                                            billrec.getLIEN_KEY().equals(lienrec.getLIEN_KEY()) &&
                                                    billrec.getSOR_CD().equals(lienrec.getSOR_CD()) &&
                                                    billrec.getSTAT_CD().equals("ACT") &&
                                                    billrec.getTAX_BILL_BGN_YR().equals("2016") &&
                                                    ( billrec.getBILL_TYP().equals("REG") || billrec.getBILL_TYP().equals("COR"))

                                    )
                                    {
                                        for(PasBillsInst instrec : instrecs)
                                        {
                                            log.info("Executing for BillInst::Bill-Key::"+instrec.getBILL_KEY()+"::"+
                                                    instrec.getSOR_CD()+"::"+instrec.getSTAT_CD());

                                            if (
                                                    instrec.getBILL_KEY().equals(billrec.getBILL_KEY()) &&
                                                            instrec.getSOR_CD().equals(billrec.getSOR_CD()) &&
                                                            instrec.getSTAT_CD().equals("ACT")

                                            )
                                                for(PasBillAmt amtrec:amtrecs)
                                                {
                                                    log.info("Executing for BillAMt::"+amtrec.getBILL_AMT());

                                                    if(
                                                            amtrec.getPRCL_BILL_INSTL_KEY().equals(instrec.getPRCL_BILL_INSTL_KEY()) &&
                                                                    amtrec.getSOR_CD().equals(instrec.getSOR_CD()) &&
                                                                    amtrec.getSTAT_CD().equals("ACT") &&
                                                                    ( amtrec.getBILL_AMT_TYP().equals("PDB") || amtrec.getBILL_AMT_TYP().equals("BAS") || amtrec.getBILL_AMT_TYP().equals("DUE"))
                                                    )
                                                    {
                                                        log.info("Finalizing record::"+amtrec.getBILL_AMT()+"::");
                                                        amtrec.setINSTL_CD(instrec.getINSTL_CD());
                                                            out.output(amtrec);
                                                    }
                                                }
                                        }
                                    }

                                }
                            }
                        }

                    }
                })
        );
        //write the output to file
        output.apply("FlattenRecords", MapElements.via(new SimpleFunction<PasBillAmt, String>() {
            @Override
            public String apply(PasBillAmt input) {
               // PasBillAmt obj = input.ge;
                return input.getPRCL_KEY() +"," + input.getLIEN_KEY() + "," + input.getBILL_KEY() + "," +input.getBILL_AMT_TYP() + ","
                        + input.getINSTL_CD()+","+input.getBILL_AMT()+","+input.getGOOD_THRU_DT();
            }
        }))
                .apply("WriteRecordstoFile",TextIO.write().withoutSharding().to(options.getOutputPasData()));

        //Group by the install code and then sum by DUE
        //Find all the amounts with a install code of T and due
        output.apply(ParDo.of(
                new DoFn<PasBillAmt, Double>() {
                    @ProcessElement
                    public void processElement(@Element PasBillAmt input,OutputReceiver<Double> output ) {
                        if (input.getINSTL_CD().equals("T") && input.getBILL_AMT_TYP().equals("BAS"))
                            output.output(input.getBILL_AMT());
                    }
                }
                )
        ).apply(Sum.doublesGlobally())
                .apply(MapElements
                         .into(TypeDescriptors.strings())
                        .via((Double d) -> Double.toString(d)))
                .apply("Write-Aggregated-File",TextIO.write().withoutSharding().to(options.getOutputPasAggegation()+"-TOTAL.csv"));

        //Group by the install code and then sum by DUE
        //Find all the amounts with a install code of 1 and due
        output.apply(ParDo.of(
                new DoFn<PasBillAmt, Double>() {
                    @ProcessElement
                    public void processElement(@Element PasBillAmt input,OutputReceiver<Double> output ) {
                        if (input.getINSTL_CD().equals("1") && input.getBILL_AMT_TYP().equals("BAS"))
                            output.output(input.getBILL_AMT());
                    }
                }
                )
        ).apply(Sum.doublesGlobally())
                .apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via((Double d) -> Double.toString(d)))
                .apply("Write-Aggregated-File",TextIO.write().withoutSharding().to(options.getOutputPasAggegation()+"-INSTALL1.csv"));

        p1.run();
    }


}
