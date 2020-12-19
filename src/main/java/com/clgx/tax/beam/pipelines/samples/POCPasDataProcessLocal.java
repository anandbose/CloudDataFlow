
package com.clgx.tax.beam.pipelines.samples;

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
import org.apache.beam.sdk.transforms.DoFn;
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

public class POCPasDataProcessLocal {

    static String elasticUrl = "http://10.128.15.219:9200";
   // static String elasticUrl = "https://cc317dd9125743c9a2f563cf4d48dd06.int-ece-main-green-proxy.elastic.int.idap.clgxdata.com:9243";
   // static String userName = "clgx_service";
   // static String elasticPassword = "clgx_service_r0ck$";
    static Logger log = LoggerFactory.getLogger(POCPasDataProcessLocal.class);
    public static void main(String[] args) {
        pasPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(pasPipelineOptions.class);

     //options.setFileName(ValueProvider.StaticValueProvider.of(options.getFilePrefix().get()+options.getStateCounty().get()+"_"+options.getDefaultDate().get()));
        //   options.setFileName(ValueProvider.StaticValueProvider.of("/Users/anbose/MyApplications/SparkPOCFiles/PAS/PRCL_STCN"+"02003"+"_"+"20201207"));
     //   log.info("File Name is::"+options.getFileName().get());
        runPasPipeline(options);

    }

    public interface pasPipelineOptions extends PipelineOptions {


        @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/-02003-20201207")
        ValueProvider<String> getFilePrefix();
        void setFilePrefix(ValueProvider<String> fileName);

        ValueProvider<String> getFileName();
        void setFileName(ValueProvider<String> fileName);

        @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/PAS/out-02003-20201207")
        ValueProvider<String> getOutputFileName();
        void setOutputFileName(ValueProvider<String> fileName);

        @Default.String("http://localhost:9200")
        ValueProvider<String> getElasticUrl();
        void setElasticUrl(ValueProvider<String> url);
    }

    public   static void runPasPipeline(pasPipelineOptions options)
    {

        Pipeline p1 = Pipeline.create(options);
        String delimiter="|";
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
                        out.output(kvObj);
                    }
                }
        ));

        /**
         * Read the PAS Parcel owner and store data in pcollection
         */


        String pasPrclOwnPrefix = "PAS_PRCL_OWN_STCN";

        PCollection<KV<String, PasPrclOwn>> parcelOwners = p1.apply("Read PAS Parcel Owner", TextIO.read().from(
                ValueProvider.NestedValueProvider.of(options.getFilePrefix(),  new SerializableFunction<String, String>()
                {
                    @Override
                    public String apply(String input) {
                        String[] fields = input.split("-");

                        return fields[0]+pasPrclOwnPrefix+fields[1]+"_"+fields[2];
                    }
                })
                )
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
                ValueProvider.NestedValueProvider.of(options.getFilePrefix(),  new SerializableFunction<String, String>()
                {
                    @Override
                    public String apply(String input) {
                        String[] fields = input.split("-");
                        String dt = fields[2];

                        return fields[0]+pasPrclLiensPrefix+fields[1]+"_"+fields[2];
                    }
                })
                )
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
                ValueProvider.NestedValueProvider.of(options.getFilePrefix(),  new SerializableFunction<String, String>()
                {
                    @Override
                    public String apply(String input) {
                        String[] fields = input.split("-");
                        String dt = fields[2];

                        return fields[0]+pasPrclBillsPrefix+fields[1]+"_"+fields[2];
                    }
                })
                )
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
                ValueProvider.NestedValueProvider.of(options.getFilePrefix(),  new SerializableFunction<String, String>()
                {
                    @Override
                    public String apply(String input) {
                        String[] fields = input.split("-");
                        String dt = fields[2];

                        return fields[0]+pasPrclBillsInstPrefix+fields[1]+"_"+fields[2];
                    }
                })
                )
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
                ValueProvider.NestedValueProvider.of(options.getFilePrefix(),  new SerializableFunction<String, String>()
                {
                    @Override
                    public String apply(String input) {
                        String[] fields = input.split("-");
                        String dt = fields[2];

                        return fields[0]+pasPrclBillsAmtPrefix+fields[1]+"_"+fields[2];
                    }
                })
                )
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

        /**
         * Take the joined data and create a JSON outof them . The JSON is based on the output desired structure
         * As part of the POC this will be a very simple data dump
         */

        PCollection<KV <String, Parcel> >output = groupedCollection.apply("Add additional Filters to get the joined records .. ",
                ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, Parcel>>() {
                    @ProcessElement
                    public void processElement(@Element  KV<String,CoGbkResult> input, OutputReceiver<KV<String, Parcel>> out)
                    {
                        Iterable<PasPrcl> PasPrclrecs = input.getValue().getAll(pasPrcltuple);
                        Iterable<PasPrclOwn> PasPrcslOwnrecs = input.getValue().getAll(pasprclowntuple);
                        Iterable<PasLiens> PasLienrecs = input.getValue().getAll(lientuple);
                        Iterable<PasBills> PasBillrecs = input.getValue().getAll(billtuple);
                        Iterable<PasBillsInst> PasBillInstrecs = input.getValue().getAll(insttuple);
                        Iterable<PasBillAmt> PasAmtrecs = input.getValue().getAll(amttuple);

                        /**
                         * Iterate each Tuple based on the joins and then do further filtering to produce the output
                         * object
                         */
                        for (PasPrcl parcel:PasPrclrecs)
                        {
                            //filter only TXA records
                            if(parcel.getSOR_CD().equals("TXA"))
                            {
                                //create the output object
                                Parcel opParcel = new Parcel();
                                List<Owner>  ownerList = new ArrayList<Owner>();
                                List<Installment> installmentList = new ArrayList<Installment>();
                                opParcel.setPrclKey(parcel.getPRCL_KEY());
                                opParcel.setStateCounty(parcel.getSTATE_ID()+parcel.getCNTY_ID());
                                Address opAddress = new Address();
                                opAddress.setStreetAddress(parcel.getSTRT_NBR_TXT()+" "+parcel.getSTRT_NM());
                                opAddress.setCity(parcel.getCITY_NM());
                                opAddress.setPostalCode(parcel.getPOSTAL_CD());
                                opParcel.setAddress(opAddress);

                                //iterate liens
                                for (PasLiens lien: PasLienrecs)
                                {
                                    opParcel.setTaxId(lien.getTAX_ID());
                                    if (parcel.getSOR_CD().equals(lien.getSOR_CD()))
                                    {
                                        //join Bills
                                        for(PasBills Bill : PasBillrecs) {
                                            if (Bill.getSOR_CD().equals("TXA")
                                                    && Bill.getLIEN_KEY().equals(lien.getLIEN_KEY())
                                            )
                                            {
                                                //join Bill Installments
                                                opParcel.setBillYear(Bill.getTAX_BILL_BGN_YR());
                                                for (PasBillsInst Inst: PasBillInstrecs)
                                                {
                                                    if(
                                                            Inst.getSOR_CD().equals("TXA") &&
                                                             Inst.getBILL_KEY().equals(Bill.getBILL_KEY())    &&
                                                             Inst.getLIEN_KEY().equals(Bill.getLIEN_KEY())

                                                    )
                                                    {
                                                        //create new installment record
                                                        Installment opInstallment = new Installment();
                                                        opInstallment.setInstallmentID(Inst.getINSTL_CD());
                                                        opInstallment.setInstallmentType(Inst.getPYMT_STAT_CD());
                                                        opInstallment.setInstallmentUniqueKey(
                                                                parcel.getPRCL_KEY() + "-" +lien.getLIEN_KEY() +"-"
                                                                + Bill.getBILL_KEY() + "-"+ Inst.getPRCL_BILL_INSTL_KEY()

                                                        );
                                                        //now add the amounts
                                                        List<Amount> amounts = new ArrayList<>();
                                                        for (PasBillAmt amount : PasAmtrecs)
                                                        {
                                                            if(
                                                            amount.getSOR_CD().equals("TXA") &&
                                                                    amount.getLIEN_KEY().equals(lien.getLIEN_KEY()) &&
                                                            amount.getBILL_KEY().equals(Bill.getBILL_KEY())    &&
                                                            amount.getPRCL_BILL_INSTL_KEY().equals(Inst.getPRCL_BILL_INSTL_KEY()) )
                                                            {
                                                                //create amount record and add it to the list
                                                                Amount opAmount = new Amount();
                                                                try {
                                                                    opAmount.setAmount(Double.valueOf((amount.getBILL_AMT() != null || amount.getBILL_AMT() != "") ? amount.getBILL_AMT() : "0.0"));
                                                                }
                                                                catch (Exception ex)
                                                                {
                                                                    ex.printStackTrace();
                                                                    opAmount.setAmount(Double.valueOf("0.0"));
                                                                }
                                                                opAmount.setAmountType(amount.getBILL_AMT_TYP());
                                                                amounts.add(opAmount);
                                                                if (opAmount.getAmountType().equals("DUE"))
                                                                {
                                                                    opInstallment.setAmountTotals(opAmount.getAmount());
                                                                }
                                                            }
                                                        }
                                                        opInstallment.setAmounts(amounts);

                                                        installmentList.add(opInstallment);

                                                    }
                                                }
                                                opParcel.setInstallments(installmentList);
                                            }
                                        }
                                    }
                                }




                                for (PasPrclOwn powner: PasPrcslOwnrecs)
                                {
                                    if(
                                          powner.getSOR_CD().equals("TXA")
                                    )
                                    {
                                        Owner opOwners = new Owner();
                                        opOwners.setFirstName(powner.getFRST_NM());
                                        opOwners.setLastName(powner.getLAST_NM());
                                        opOwners.setOwnerType(powner.getOWN_TYP());
                                        opOwners.setOwnerKey(powner.getOWN_KEY());
                                        ownerList.add(opOwners);


                                    }
                                }
                                opParcel.setOwners(ownerList);

                                out.output(KV.of(opParcel.getPrclKey(),opParcel));

                            }
                        }

                    }

                })
                );

        /***
         * Write a normalized parcel output
         */
        PCollection<Parcel> opParcels = output.apply(ParDo.of(new DoFn<KV<String, Parcel>, Parcel>() {
            @ProcessElement
            public void processElement(@Element  KV<String,Parcel> input, OutputReceiver< Parcel> out)
            {
                out.output(input.getValue());
            }

        }));

       PCollection<String> jsonString =  opParcels.apply("ConvertoJson", AsJsons.of(Parcel.class));
                        jsonString.apply("Write Records to File",TextIO.write().withoutSharding().to(options.getOutputFileName()));

        ElasticsearchIO.ConnectionConfiguration connectionConfiguration = null;
          connectionConfiguration =ElasticsearchIO.ConnectionConfiguration.create(new String[]{elasticUrl}, "pas-poc-data", "prcls")
                                   //     .withUsername(userName)
                                     //       .withPassword(elasticPassword)
          ;

        jsonString.apply("write to elastic", ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration)
                .withIdFn(new ElasticsearchIO.Write.FieldValueExtractFn() {
                    @Override
                    public String apply(JsonNode input) {
                        String prclKey = input.get("prclKey").asText();
                        String taxId = input.get("taxId").asText();
                        return prclKey+"-"+taxId;
                    }
                })
                .withMaxBatchSize(10000));

        /**
         *  Write parcel data to Elastic , make sure that the doc id is unique
         */

        /**
         * Convert to installment based output
         */

        final TupleTag<OutputByInstallment> multiInstallmentTag =
                new TupleTag<OutputByInstallment>(){};
        PCollection<OutputByInstallment> opByInstallment = output.apply(ParDo.of(new DoFn<KV<String, Parcel>, OutputByInstallment>() {

             @ProcessElement
             public void processElement(@Element KV<String,Parcel> input, OutputReceiver<OutputByInstallment> out)
             {
                 Parcel prcl = input.getValue();
                 List<OutputByInstallment> finalOp = new ArrayList<>();
                 List<Installment> installments = prcl.getInstallments();
                 if (installments!=null && installments.size()>0){
                 for (Installment installment : installments)
                 {
                     OutputByInstallment op = new OutputByInstallment();

                     //create the output installment
                     op.setPrclKey(prcl.getPrclKey());
                     op.setAddress(prcl.getAddress());
                     op.setOwners(prcl.getOwners());
                     op.setStateCounty(prcl.getStateCounty());
                     op.setInstallmentID(installment.getInstallmentID());
                     op.setInstallmentType(installment.getInstallmentType());
                     op.setAmounts(installment.getAmounts());
                     op.setAmountTotals(installment.getAmountTotals());
                     op.setInstallmentUniqueKey(installment.getInstallmentUniqueKey());
                     op.setBillYear(prcl.getBillYear());
                     out.output(op);
                    // finalOp.add(op);

                 }
                // out.output(finalOp);

             }}

        }));
      //  PCollectionList<OutputByInstallment> finalCollection = PCollectionList.of(opByInstallmentList);
        PCollection<String> jsonString1 =opByInstallment.apply("ConvertoJson", AsJsons.of(OutputByInstallment.class));
        jsonString1.apply("Write installment Records to File",TextIO.write().withoutSharding().to(ValueProvider.NestedValueProvider.of(options.getOutputFileName(),
                        new SerializableFunction<String, String>() {
                            @Override
                            public String apply(String input) {
                                return input+"-installment-";
                            }
                        })));
        /*
         Write to Elastic as well
         */
        ElasticsearchIO.ConnectionConfiguration connectionConfiguration2 = ElasticsearchIO.ConnectionConfiguration.create(new String[]{elasticUrl}, "pas-instl-poc-data", "installment")
                  //  .withUsername(userName)
                   // .withPassword(elasticPassword)
        ;

        jsonString1.apply("write to elastic", ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration2)
                .withIdFn(new ElasticsearchIO.Write.FieldValueExtractFn() {
                    @Override
                    public String apply(JsonNode input) {

                        return input.get("installmentUniqueKey").asText();
                    }
                })
                .withMaxBatchSize(10000));

        /**
         * Run the pipeline
         */
        p1.run();
    }
}
