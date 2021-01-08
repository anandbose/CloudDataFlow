package com.clgx.tax.beam.pipelines.poc.sdp;

import com.clgx.tax.data.model.poc.PasPrcl;
import com.clgx.tax.services.httpclient.SdpEclipseHttpClient;
import com.clgx.tax.services.httpclient.SynchHttpClient;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HttpWriterPlain<T> extends PTransform<PCollection<KV<String,PasPrcl>>,PCollection<KV<String,PasPrcl>> >{


    static Logger log = LoggerFactory.getLogger(HttpWriterPlain.class);

    @Override
    public PCollection<KV<String,PasPrcl> > expand(PCollection<KV<String,PasPrcl>> input) {
        return input.apply(ParDo.of(new Fn<>()));
       // return null;
    }

    private  static class Fn<T> extends DoFn<KV<String,PasPrcl>, KV<String,PasPrcl>> {
        private SynchHttpClient synchClient;

        private Fn() {

        }

        @Setup
        public void onSetup() {
            this.synchClient = new SynchHttpClient();
        }


        @ProcessElement
        public void onElement(ProcessContext context
        ) {

            //  expiryTimer.set(window.maxTimestamp().plus(Duration.standardMinutes(5)));

            // PasPrcl obj = enrichEvents(context.element().getValue());
            //KV<String,PasPrcl> keyval = KV.of(obj.getPRCL_KEY(),obj);
            //context.output(keyval);
            for (PasPrcl enrichedEvent : enrichEvents(context.element())) {
                KV<String,PasPrcl> parcelKV = KV.of(enrichedEvent.getPRCL_KEY(),enrichedEvent);
                log.info("Enriched Parcel is :: "+enrichedEvent.toString());
                context.output(parcelKV);
            }

           // context.output(context.element());
            //  context.output(outputObj);

        }
        private  List<PasPrcl> enrichEvents(KV<String,PasPrcl> parcels){
            List<PasPrcl> outPut = new ArrayList<>();
            PasPrcl prcl = parcels.getValue();


                String output =this.synchClient.send(prcl.getASSESS_PRCL_ID(),
                        prcl.getSTRT_NBR_TXT() + " " + prcl.getSTRT_NM() + " " + prcl.getSTRT_TYP(),
                        prcl.getCITY_NM(),
                        prcl.getSTATE_CD(),
                        prcl.getPOSTAL_CD()
                );
                log.info("Iterating for parcekl::"+prcl.getPRCL_KEY());
                 PasPrcl outputObj = new PasPrcl();

            try {
                outputObj = (PasPrcl) prcl.clone();
                        JSONObject obj = new JSONObject(output);
                        JSONArray eclipse = obj.getJSONArray("data");
                        String clipNumber;


                        for (int i = 0; i < eclipse.length();i++ ) {
                            JSONObject clipObject = eclipse.getJSONObject(i);
                            clipNumber = clipObject.getString("clip");
                            outputObj.setClipNumber(clipNumber);


                        }
                        log.info("Iterating for parcekl::"+outputObj.toString());

                        outPut.add(outputObj);
                }
                     catch (Exception ex)
                     {
                         outPut.add(outputObj);
                         ex.printStackTrace();
                 }
            finally
            {
                return outPut;
            }



          //  return outPut;
        }
    }

}


