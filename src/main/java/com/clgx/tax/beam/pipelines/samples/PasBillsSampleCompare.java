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

import com.google.common.hash.Hashing;
import com.clgx.tax.data.model.PasBills;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
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

import java.nio.charset.StandardCharsets;


public class PasBillsSampleCompare {



  public interface PasBillsOptions extends PipelineOptions {


    @Description("Path of the file to read from")
    @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/MaricopaCounty/Unzipped/training_data_pasbills.csv")
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
     //  Pipeline p2 = Pipeline.create(options);

       // Create row schema
       // Define the schema for the records.


       PCollection<KV<String,PasBills>> TodayFile = p1.apply("ReadTodaysFile-PasBills", TextIO.read().from(options.getTodaysFile1()))
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

       PCollection<KV<String,PasBills>> YesterdaysFile = p1.apply("ReadYesterdaysFile-PasBills", TextIO.read().from(options.getYesterdaysFile1()))
               .apply("ConverttoPCollection",ParDo.of(new DoFn<String, KV<String,PasBills>>() {
                   @ProcessElement
                   // @DefaultCoder(Avi)
                   public void processElement(@Element String Input , OutputReceiver<KV<String,PasBills>> out)
                   {
                       String[] field = Input.split(",");
                   //    if (field.length < 10) return;

                       PasBills obj=new PasBills();
                       obj.setPRCL_KEY(field[1]);
                       obj.setSOR_CD(field[2]);
                       obj.setLIEN_KEY(field[3]);
                       obj.setBILL_KEY(field[4]);
                       obj.setLAST_UPDT_TS(field[5]);

                       obj.setHashKey(field[0]);
                       KV<String,PasBills> kvObj = KV.of(field[0],obj);

                       out.output(kvObj);
                   }
               }));

       final TupleTag<PasBills> TodayFileDataset1 = new TupleTag<>();
       final TupleTag<PasBills> YesFileDataset2 = new TupleTag<>();
       PCollection<KV<String, CoGbkResult>> groupedCollection = KeyedPCollectionTuple
               .of(TodayFileDataset1, TodayFile)
               .and(YesFileDataset2, YesterdaysFile)
               .apply("JoinDataViaHashKeys",CoGroupByKey.create());

        PCollection<KV<String , PasBills>> op = groupedCollection.apply("IdentifyDeltaRecords-InsertorUpdates",
                ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, PasBills>>() {
                    @ProcessElement
                     public void processElement( @Element KV<String, CoGbkResult> input , OutputReceiver<KV<String, PasBills>> Output) {
                        Iterable<PasBills> TodaysData = input.getValue().getAll(TodayFileDataset1);
                        Iterable<PasBills> YeserData = input.getValue().getAll(YesFileDataset2);
                        for (PasBills BillToday: TodaysData)
                        {
                            int iCounter=0;
                            for (PasBills BillYesterday: YeserData)
                            {
                                if (BillToday.getHashKey().equals(BillYesterday.getHashKey()))
                                    iCounter++;
                              //  System.out.println("Iterating::"+BillToday.getHashKey()+"::"+BillYesterday.getHashKey());

                            }
                            if (iCounter == 0)
                            {
                                System.out.println("New data::"+BillToday.getHashKey());
                                KV<String,PasBills> kv = KV.of(input.getKey(), BillToday);
                                Output.output(kv);
                            }
                        }
                    }
                }));

          PCollection<String> strOp = op.apply("FlattenDeltaRecords",MapElements.via(new SimpleFunction<KV<String, PasBills>, String>() {
                    @Override
                    public String apply(KV<String,PasBills> input)
                    {
                        PasBills obj = input.getValue();
                        return obj.getHashKey() + ","+obj.getPRCL_KEY()+","+ obj.getSOR_CD()+","+ obj.getLIEN_KEY()+","+ obj.getBILL_KEY()+","+ obj.getLAST_UPDT_TS();
                    }
                }));
                   strOp.apply("WriteDeltaRecordstoFile",TextIO.write().withoutSharding().to(options.getOutput()));
               //    strOp.apply("append to file....",TextIO.write().withoutSharding().to(options.getYesterdaysFile1()));
       TodayFile.apply("ReplaceYesterdaysDatawithTodays",MapElements.via(new SimpleFunction<KV<String, PasBills>, String>() {
           @Override
           public String apply(KV<String,PasBills> input)
           {
               PasBills obj = input.getValue();
               return obj.getHashKey() + ","+obj.getPRCL_KEY()+","+ obj.getSOR_CD()+","+ obj.getLIEN_KEY()+","+ obj.getBILL_KEY()+","+ obj.getLAST_UPDT_TS();
           }
       })).apply("ReplaceYesterdaysFilewithTodays",TextIO.write().withoutSharding().to(options.getYesterdaysFile1()));
       //*find records which are deleted

       PCollection<KV<String , PasBills>> delRecs = groupedCollection.apply("IdentifyDeleteRecords",
               ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, PasBills>>() {
                   @ProcessElement
                   public void processElement( @Element KV<String, CoGbkResult> input , OutputReceiver<KV<String, PasBills>> Output) {
                       Iterable<PasBills> TodaysData = input.getValue().getAll(TodayFileDataset1);
                       Iterable<PasBills> YeserData = input.getValue().getAll(YesFileDataset2);
                       for (PasBills BillYesterday: YeserData )
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
                               KV<String,PasBills> kv = KV.of(input.getKey(), BillYesterday);
                               Output.output(kv);
                           }
                       }
                   }
               }));
       delRecs.apply("FlattenDeleteRecords",MapElements.via(new SimpleFunction<KV<String, PasBills>, String>() {
       @Override
       public String apply(KV<String,PasBills> input)
       {
           PasBills obj = input.getValue();
           return obj.getHashKey() + ","+obj.getPRCL_KEY()+","+ obj.getSOR_CD()+","+ obj.getLIEN_KEY()+","+ obj.getBILL_KEY()+","+ obj.getLAST_UPDT_TS();
       }
   }))
               .apply("WriteDeleteRecordstoFile",TextIO.write().withoutSharding().to(options.getDelOutput()));

p1.run();



   }
  public static void main(String[] args) {
    PasBillsOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PasBillsOptions.class);

    runPasPipeline(options);
  }
}
