package com.clgx.tax.beam.pipelines.poc.sdp;

import com.clgx.tax.data.model.poc.PasPrcl;
import com.clgx.tax.services.httpclient.SimpleAsychHttp;
import com.clgx.tax.services.httpclient.SynchHttpClient;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HttpWriterAsynch<T> extends PTransform<PCollection<KV<String,PasPrcl>>,PCollection<KV<String,PasPrcl>> > {


    static Logger log = LoggerFactory.getLogger(HttpWriterAsynch.class);

    @Override
    public PCollection<KV<String, PasPrcl>> expand(PCollection<KV<String, PasPrcl>> input) {
        return input.apply(ParDo.of(new Fn<>()));
        // return null;
    }

    private static class Fn<T> extends DoFn<KV<String, PasPrcl>, KV<String, PasPrcl>> {
         private SimpleAsychHttp asynchClient;
        //private SynchHttpClient synchClient;

        private Fn() {

        }

        @Setup
        public void onSetup() {
            this.asynchClient = new SimpleAsychHttp();
        }

        private static final int MAX_BUFFER_SIZE = 1000;


        @StateId("buffer")
        private final StateSpec<BagState<PasPrcl>> bufferedEvents = StateSpecs.bag();

        @StateId("count")
        private final StateSpec<ValueState<Integer>> countState = StateSpecs.value();
        @TimerId("expiry")
        private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

        @ProcessElement
        public void onElement(final ProcessContext context,
                              BoundedWindow window,

                              @StateId("buffer") BagState<PasPrcl> bufferState,
                              @StateId("count") ValueState<Integer> countState,
                              @TimerId("expiry") Timer expiryTimer) {

            //  expiryTimer.set(window.maxTimestamp().plus(Duration.standardMinutes(5)));
            int count = countState.read() != null ? countState.read() : 0;
            count = count + 1;
            countState.write(count);
            bufferState.add(context.element().getValue());
            log.info("Buffering ::" + bufferState.read().iterator().next().getPRCL_KEY());
            expiryTimer.set(window.maxTimestamp().plus(Duration.standardSeconds(10)));
            // expiryTimer.set(Instant.now().plus(Duration.standardSeconds(10)));
            // PasPrcl element = context.element();

            log.info("Event being buffered:::");


            if (count >= MAX_BUFFER_SIZE) {

                for (PasPrcl enrichedEvent : enrichEvents(bufferState.read())) {
                    KV<String, PasPrcl> parcelKV = KV.of(enrichedEvent.getPRCL_KEY(), enrichedEvent);

                    log.info("Processing inside buffer :: Enriched Parcel is :: " + enrichedEvent.toString());
                    context.output(parcelKV);
                }
                //  context.output(outputObj);
                bufferState.clear();
                countState.clear();
            }


        }

        @OnTimer("expiry")
        public void onExpiry(
                OnTimerContext context, BoundedWindow window,
                @StateId("buffer") BagState<PasPrcl> bufferState) {
            log.info("Event Expired:::Now in the timer process");
            // log.info("Current buffer is::"+bufferState.read().iterator().next().getPRCL_KEY());

            if (!bufferState.isEmpty().read()) {
                for (PasPrcl enrichedEvent : enrichEvents(bufferState.read())) {
                    log.info("Current buffer is::" + enrichedEvent.getPRCL_KEY());
                    KV<String, PasPrcl> parcelKV = KV.of(enrichedEvent.getPRCL_KEY(), enrichedEvent);
                    context.outputWithTimestamp(parcelKV, window.maxTimestamp());
                    log.info("Event Expired:::Enriched parcel is ::" + enrichedEvent.toString());

                }
                bufferState.clear();
            }
        }

        private List<PasPrcl> enrichEvents(Iterable<PasPrcl> parcels) {
            List<PasPrcl> outPut = new ArrayList<>();
            for (PasPrcl prcl : parcels) {

                String output = this.asynchClient.send(prcl.getASSESS_PRCL_ID(),
                        prcl.getSTRT_NBR_TXT() + " " + prcl.getSTRT_NM() + " " + prcl.getSTRT_TYP(),
                        prcl.getCITY_NM(),
                        prcl.getSTATE_CD(),
                        prcl.getPOSTAL_CD()
                );
                log.info("Iterating for parcel::" + prcl.getPRCL_KEY());
                PasPrcl outputObj = new PasPrcl();

                try {
                    outputObj = (PasPrcl) prcl.clone();
                    if (!output.equals(""))
                    {
                        JSONObject obj = new JSONObject(output);
                        JSONArray eclipse = obj.getJSONArray("data");
                        String clipNumber;



                        for (int i = 0; i < eclipse.length(); i++) {
                            JSONObject clipObject = eclipse.getJSONObject(i);
                            clipNumber = clipObject.getString("clip");
                            outputObj.setClipNumber(clipNumber);


                        }
                    }

                    log.info("Iterating for parcel::" + outputObj.toString());

                    outPut.add(outputObj);
                } catch (Exception ex) {
                    outPut.add(outputObj);

                    log.error("Error while creating output::"+ex.getMessage());
                }

            }
            return outPut;
        }
    }
}




