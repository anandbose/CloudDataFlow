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

import com.clgx.tax.data.model.PasBills;
import com.clgx.tax.data.model.nts.BotSchema;
import com.clgx.tax.data.model.nts.Diablo;
import com.clgx.tax.data.model.nts.Mergedoutput;
import com.clgx.tax.data.model.poc.PasPrcl;
import com.clgx.tax.data.model.poc.PasPrclOwn;
import com.clgx.tax.data.model.poc.output.OutputByInstallment;
import com.clgx.tax.data.model.poc.output.Parcel;
import com.clgx.tax.mappers.nts.NTSTransformations;
import com.google.common.hash.Hashing;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;


public class NTSDiabloValueProduct {
    static Logger log  = LoggerFactory.getLogger(NTSDiabloValueProduct.class);

    public static void main(String[] args) {
        NTSOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(NTSOptions.class);

        runPasPipeline(options);
    }

  public interface NTSOptions extends PipelineOptions {


    @Description("Diablo file")
    @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/NTS-Diablo/input/diablo/diablodata.txt")
    ValueProvider<String> getDiabloFile();

    void setDiabloFile(ValueProvider<String> value);


    @Description("Path of the file to read from")
    @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/NTS-Diablo/input/bot/botfile.out")
    ValueProvider<String> getBotFile();
    void setBotFile(ValueProvider<String> value);

    @Description("Path to outbound  file")
      @Default.String("/Users/anbose/MyApplications/SparkPOCFiles/NTS-Diablo/output/")
      ValueProvider<String> getOutputExceptionFile();
      void setOutputExceptionFile(ValueProvider<String> value);


  }
/**Build pipeline*/
   public static void runPasPipeline(NTSOptions options)
   {
            Pipeline p1 = Pipeline.create(options);

            /*
            * Create the reader for todays file. Read each line , split by csv and convert the lines into
            * PasBill record and then to collection. Each pasBill record is hashed (hashkey created using a combination of
            * 3-4 fields) . The PCollection object is the output of this and it will be a key value pair of hash key to pasbills object
            *
            * */
             PCollection<KV<String, Diablo>> diablodata =
                p1.apply("Read Diablo data", TextIO.read().from(options.getDiabloFile()))
                .apply("Convert String to record" , ParDo.of(new DoFn<String, KV<String, Diablo>>() {
                                                                 @ProcessElement
                                                                 public void processElement(@Element String Input, OutputReceiver<KV<String, Diablo>> out) {
                                                                     String[] fields = Input.split("\t");
                                                                     //converto Diablo
                                                                     try {
                                                                         Diablo obj = new NTSTransformations().mapToDiablo(fields);
                                                                         KV<String, Diablo> keyval = KV.of(obj.getClient_Supplied_APN(), obj);
                                                                         out.output(keyval);
                                                                     }
                                                                     catch (Exception ex)
                                                                     {
                                                                        // ex.printStackTrace();
                                                                         log.error("TheString Errored out::"+Input+"::");

                                                                     }

                                                                 }
                                                             }
                ));

       /**
        * Process the bot data
         */
       PCollection<KV<String, BotSchema>> botdata =           p1.apply("Read bot data", TextIO.read().from(options.getBotFile()))
               .apply("Convert String to record" , ParDo.of(new DoFn<String, KV<String, BotSchema>>() {
                                                                @ProcessElement
                                                                public void processElement(@Element String Input, OutputReceiver<KV<String, BotSchema>> out) {
                                                                    String modified=Input.replaceAll("\"","");
                                                                    String[] fields = modified.split(",");
                                                                    //converto bost schema
                                                                    try {
                                                                        BotSchema obj = new NTSTransformations().maptoBot(fields);
                                                                        KV<String, BotSchema> keyval = KV.of(obj.getParcelID(), obj);
                                                                        out.output(keyval);
                                                                    }
                                                                    catch (Exception ex)
                                                                    {
                                                                        // ex.printStackTrace();
                                                                        log.error("TheString Errored out::"+Input+"::");

                                                                    }

                                                                }
                                                            }
               ));

       /**Run the pipeline**/


       /**
        * Find exception records within diablo data
        */
       diablodata.apply("filter incomplete records",Filter.by(new SerializableFunction<KV<String, Diablo>, Boolean>() {
           @Override
           public Boolean apply(KV<String, Diablo> input) {
               if (!input.getValue().getProperty_Match_Code().equals("H099") && !input.getValue().getClient_ID().contains("Client")) {
                   return true;
               }
               else return false;
           }
       })).apply("create Exception records",ParDo.of(new DoFn<KV<String, Diablo>, String>() {
           @ProcessElement
           public void processElement(@Element KV<String,Diablo> input , OutputReceiver<String> out)
           {
               Diablo obj = input.getValue();
               out.output(obj.getClient_ID()+"\t"+obj.getClient_Supplied_APN()+"\t"+obj.getProperty_Match_Code()+"\t"+obj.getMatch_Exception_Code());
           }

       })).apply("Write Exception to File",TextIO.write().withoutSharding().to(ValueProvider.NestedValueProvider.of(options.getOutputExceptionFile(),
               new SerializableFunction<String,String>(){

                   @Override
                   public String apply(String input) {
                       Date dt = new Date(System.currentTimeMillis());
                       SimpleDateFormat formatter= new SimpleDateFormat("yyyyMMdd");

                       return input +"DiabloException-"+formatter.format(dt)+".txt";
                   }
               })));

       /**
        * Joining Diablo and bot data
        */
       final TupleTag<Diablo> diabloTuple = new TupleTag<>();
       final TupleTag<BotSchema>  botTuple = new TupleTag<>();
       PCollection<KV<String, CoGbkResult>> groupedCollection = KeyedPCollectionTuple
    .of(diabloTuple,diablodata)
     .and(botTuple,botdata)
               .apply("Join-Data-Via-APN", CoGroupByKey.create());

       PCollection<Mergedoutput> merged = groupedCollection.apply(
               "Join and filter",ParDo.of(new DoFn<KV<String, CoGbkResult>, Mergedoutput>() {
                   @ProcessElement
                   public void processElement(@Element  KV<String,CoGbkResult> input, OutputReceiver <Mergedoutput> out){
                       Iterable<Diablo> setDiablo = input.getValue().getAll(diabloTuple);
                       Iterable<BotSchema> setBot = input.getValue().getAll(botTuple);
                       //Mergedoutput m = new Mergedoutput();
                       for (Diablo objDiablo : setDiablo)
                       {
                           if(objDiablo.getProperty_Match_Code().equals("H099"))
                           for(BotSchema objBot : setBot)
                           {
                               if((objBot.getTotalInitialAssessedValue() != null || objBot.getTotalInitialAssessedValue() != "")

                                  && (objBot.getTotalInitialMarketValue() != null || objBot.getTotalInitialMarketValue() != ""))
                              out.output(new NTSTransformations().getMerged(objDiablo,objBot));
                           }
                       }

                   }

               })
       );

        merged.apply("convert to String",ParDo.of(new DoFn<Mergedoutput, String>() {
            @ProcessElement
            public void processElement(@Element Mergedoutput input,OutputReceiver<String> out)
            {
                out.output(new NTSTransformations().converttoDelimitedString(input,"\t"));
            }
        })).apply("Write output to File",TextIO.write().withoutSharding().withHeader(new NTSTransformations().getOutputHeader()).to(ValueProvider.NestedValueProvider.of(options.getOutputExceptionFile(),
                new SerializableFunction<String,String>(){

                    @Override
                    public String apply(String input) {
                        Date dt = new Date(System.currentTimeMillis());
                        SimpleDateFormat formatter= new SimpleDateFormat("yyyyMMdd");

                        return input +"MergedFile-"+formatter.format(dt)+".txt";
                    }})));

       p1.run();



   }

}
