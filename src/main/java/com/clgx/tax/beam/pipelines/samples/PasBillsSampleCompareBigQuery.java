/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.clgx.tax.beam.pipelines.samples;

import com.clgx.tax.data.model.BQPasBills;
import com.clgx.tax.data.model.PasBills;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.hash.Hashing;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.Validation.Required;
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
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;


public class PasBillsSampleCompareBigQuery {



  public interface PasBillsOptions extends PipelineOptions {


    @Description("Path of the file to read from")
    @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/MaricopaCounty/Unzipped/testfiles/new_test_example.csv")
    ValueProvider<String> getTodaysFile1();

    void setTodaysFile1(ValueProvider<String> value);


      @Description("Path of the file to read from")
      @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/MaricopaCounty/Unzipped/beam-pasbills-out.csv")
      ValueProvider<String> getYesterdaysFile1();

      void setYesterdaysFile1(ValueProvider<String> value);

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Required
    @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/MaricopaCounty/Unzipped/beam-deltas-out.csv")
    ValueProvider<String> getOutput();

    void setOutput(ValueProvider<String> value);


      @Description("Path of the file to write to")
      @Required
      @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/MaricopaCounty/Unzipped/beam-deletes-out.csv")
      ValueProvider<String> getDelOutput();

      void setDelOutput(ValueProvider<String> value);
  }
/**Build pipeline*/
   public static void runPasPipeline(PasBillsOptions options)
   {
            Pipeline p1 = Pipeline.create(options);
            TableReference tableSpec =
               new TableReference()
                       .setProjectId("spheric-mesh-294917")
                       .setDatasetId("demo_data")
                       .setTableId("PAS_PRCL_BILLS");
            /*
            * Create the reader for todays file. Read each line , split by csv and convert the lines into
            * PasBill record and then to collection. Each pasBill record is hashed (hashkey created using a combination of
            * 3-4 fields) . The PCollection object is the output of this and it will be a key value pair of hash key to pasbills object
            *
            * */

            PCollection<KV<String,PasBills>> TodayFile = p1.apply("Read-TodaysFile-PasBills",
                    TextIO.read().from(options.getTodaysFile1()))
                 .apply("ConvertToPCollectionandGenerateHashKey",ParDo.of(new DoFn<String, KV<String,PasBills>>() {
                     @ProcessElement
                     // @DefaultCoder(Avi)
                     public void processElement(@Element String Input , OutputReceiver<KV<String,PasBills>> out)
                     {
                         String[] field = Input.split(",");
                       //  if (field.length < 10) return;

                         PasBills obj=new PasBills();
                         obj.setPRCL_KEY(field[0]);
                         obj.setSOR_CD(field[1]);
                         obj.setLIEN_KEY(field[2]);
                         obj.setBILL_KEY(field[3]);
                         obj.setLAST_UPDT_TS(field[29]);
                         String sha256hex = Hashing.sha256()
                                 .hashString(field[0]+"||"+field[1]+"||"+field[2]+"||"+field[3], StandardCharsets.UTF_8)
                                 .toString();
                         obj.setHashKey(sha256hex);
                         KV<String,PasBills> kvObj = KV.of(sha256hex,obj);

                         out.output(kvObj);
                     }
                 }));

           /*
            * Create the reader for yesterdays file. Read each line , split by csv and convert the lines into
            * PasBill record and then to collection. The hashkey is already part of yesterdays file
            * The PCollection object is the output of this and it will be a
            *  key value pair of hash key to pasbills object
            *
            * */

            PCollection<KV<String,BQPasBills>> YesterdaysBQData = p1.apply("ReadYesterdaysdata-fromBQ-PasBills",
                    BigQueryIO.readTableRowsWithSchema()

                            .from(tableSpec)
                    .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                    )
                   .apply("ConverttoPCollection",
                   MapElements.into(TypeDescriptor.of(BQPasBills.class)).via(BQPasBills::convertToObj)
                           )
                    .apply("add key",ParDo.of(new DoFn<BQPasBills, KV<String,BQPasBills>>(){
                        @ProcessElement
                        // @DefaultCoder(Avi)
                        public void processElement(@Element BQPasBills Input , OutputReceiver<KV<String,BQPasBills>> out)
                        {
                            KV<String,BQPasBills> op= KV.of(Input.getHashKey(),Input);
                            out.output(op);
                        }
                    }));


            /*
            * Join Today's PCollection and Yesterday's PCollection.
            * The join required 2 tuples to be created . The tuples are the datasets which
            * need to be joined (in this case PasBills)
            *
            **/
            final TupleTag<PasBills> TodayFileDataset1 = new TupleTag<>();
            final TupleTag<BQPasBills> YesFileDataset2 = new TupleTag<>();
            PCollection<KV<String, CoGbkResult>> groupedCollection = KeyedPCollectionTuple
                       .of(TodayFileDataset1, TodayFile)

                       .and(YesFileDataset2, YesterdaysBQData)
                       .apply("Join-Data-Via-HashKeys",CoGroupByKey.create());

           /*
            * Iterate through the joined Collectiom
            * Compare Todays hashkeys to yesterdays hash keys
            * Identify new hashkey in todays records and return them as output
            *
            **/

            PCollection<KV<String , BQPasBills>> op = groupedCollection.apply("Identify-DeltaRecords-InsertsorUpdates",
                    ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, BQPasBills>>() {
                        @ProcessElement
                         public void processElement( @Element KV<String, CoGbkResult> input , OutputReceiver<KV<String, BQPasBills>> Output) {
                            Iterable<PasBills> TodaysData = input.getValue().getAll(TodayFileDataset1);
                            Iterable<BQPasBills> YeserData = input.getValue().getAll(YesFileDataset2);
                            for (PasBills BillToday: TodaysData)
                            {
                                int iCounter=0;
                                for (BQPasBills BillYesterday: YeserData)
                                {
                                    if (BillToday.getHashKey().equals(BillYesterday.getHashKey()))
                                        iCounter++;
                                     System.out.println("Iterating::"+BillToday.getHashKey()+"::"+BillYesterday.getHashKey());

                                }
                                if (iCounter == 0)
                                {
                                    System.out.println("New data::"+BillToday.getHashKey());
                                    //Creatre a new Bill
                                    BQPasBills obj= new BQPasBills();
                                    obj.setHashKey( BillToday.getHashKey());
                                    obj.setParcelKey(BillToday.getPRCL_KEY());
                                    obj.setSorCD(BillToday.getSOR_CD());
                                    obj.setLienKey(BillToday.getLIEN_KEY());
                                    obj.setBillKey(BillToday.getBILL_KEY());
                                    obj.setLastUpdtTS(BillToday.getLAST_UPDT_TS());

                                    KV<String,BQPasBills> kv = KV.of(input.getKey(), obj);
                                    Output.output(kv);
                                }
                            }
                        }
                    }));

            /*
            * Convert deltarecords to table rows
            */
            PCollection<TableRow> rows= op.apply("Convert to BQ rows" , ParDo.of(new DoFn<KV<String, BQPasBills>, TableRow>() {
               @ProcessElement
               public void processElement( @Element KV<String, BQPasBills> input, OutputReceiver<TableRow> out)
               {
                   BQPasBills obj = input.getValue();
                   out.output(BQPasBills.convertToRow(obj));
               }
            }));

            /*
            * Write Delta records to BQ
            *
            **/

            rows.apply("Write to BQ",
                    BigQueryIO.writeTableRows()

                    .to(tableSpec)
                    .withSchema(BQPasBills.getPasBillsSchema())
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    );
          /*  PCollection<String> strOp = op.apply("Flatten-Delta-Records",MapElements.via(new SimpleFunction<KV<String, PasBills>, String>() {
                    @Override
                    public String apply(KV<String,PasBills> input)
                    {
                        PasBills obj = input.getValue();
                        return obj.getHashKey() + ","+obj.getPRCL_KEY()+","+ obj.getSOR_CD()+","+ obj.getLIEN_KEY()+","+ obj.getBILL_KEY()+","+ obj.getLAST_UPDT_TS();
                    }
                }));*/
           /*
            * Flatten the delta delete records
            *
            **/
           // strOp.apply("Write-Delta-Records-to-File",TextIO.write().withoutSharding().to(options.getOutput()));

       //*find records which are deleted
         PCollection<KV<String , BQPasBills>> delRecs = groupedCollection.apply("IdentifyDeleteRecords",
                  ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, BQPasBills>>() {
                   @ProcessElement
                   public void processElement( @Element KV<String, CoGbkResult> input , OutputReceiver<KV<String, BQPasBills>> Output) {
                       Iterable<PasBills> TodaysData = input.getValue().getAll(TodayFileDataset1);
                       Iterable<BQPasBills> YeserData = input.getValue().getAll(YesFileDataset2);
                       for (BQPasBills BillYesterday: YeserData )
                       {
                           int iCounter=0;
                           for (PasBills BillToday: TodaysData)
                           {
                               if (BillToday.getHashKey().equals(BillYesterday.getHashKey()))
                                   iCounter++;
                               //  System.out.println("Iterating::"+BillToday.getHashKey()+"::"+BillYesterday.getHashKey());

                           }
                           if (iCounter == 0)
                           {
                               System.out.println("Deleted data::"+BillYesterday.getHashKey());
                               KV<String,BQPasBills> kv = KV.of(input.getKey(), BillYesterday);
                               Output.output(kv);
                           }
                       }
                   }
                }));
       delRecs.apply("FlattenDeleteRecords",MapElements.via(new SimpleFunction<KV<String, BQPasBills>, String>() {
                   @Override
                   public String apply(KV<String,BQPasBills> input) {
                         BQPasBills obj = input.getValue();
                         return obj.getHashKey() + ","+obj.getParcelKey()+","+ obj.getSorCD()+","+ obj.getLienKey()+","+ obj.getBillKey()+","+ obj.getLastUpdtTS();
                     }
                }))
               .apply("WriteDeleteRecordstoFile",TextIO.write().withoutSharding().to(options.getDelOutput()));
       /**Run the pipeline**/

       p1.run();



   }
  public static void main(String[] args) {
    PasBillsOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PasBillsOptions.class);

    runPasPipeline(options);
  }
}
