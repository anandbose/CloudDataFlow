package com.clgx.tax.poc.clip.pipeline;

import com.clgx.tax.poc.clip.model.PasPrcl;
import com.clgx.tax.poc.clip.services.SynchHttpClient;
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

public class HttpWriter<T> extends PTransform<PCollection<KV<String,PasPrcl>>,PCollection<KV<String,PasPrcl>> > {

    //private static String url;
    static Logger log = LoggerFactory.getLogger(HttpWriter.class);

    @Override
    public PCollection<KV<String, PasPrcl>> expand(PCollection<KV<String, PasPrcl>> input) {
        return input.apply(ParDo.of(new Fn<>()));
    }

    private static class Fn<T> extends DoFn<KV<String, PasPrcl>, KV<String, PasPrcl>> {
        private SynchHttpClient synchClient;

        private Fn() {

        }

        @Setup
        public void onSetup() {
            this.synchClient = new SynchHttpClient();
        }

        private static final int MAX_BUFFER_SIZE = 15;


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
                              @TimerId("expiry") Timer expiryTimer) throws InterruptedException {

            //  expiryTimer.set(window.maxTimestamp().plus(Duration.standardMinutes(5)));
            int count = countState.read() != null ? countState.read() : 0;
            count = count + 1;
            countState.write(count);
            bufferState.add(context.element().getValue());
       //     log.info("Buffering ::" + bufferState.read().iterator().next().getPRCL_KEY());
         //   log.info("Expiry window is::"+window.maxTimestamp().plus(Duration.standardSeconds(10)));
            expiryTimer.set(window.maxTimestamp().plus(Duration.standardSeconds(10)));
            //Thread.sleep(71000);
            // expiryTimer.set(Instant.now().plus(Duration.standardSeconds(10)));
            // PasPrcl element = context.element();

           // log.info("Event being buffered:::");


            if (count >= MAX_BUFFER_SIZE) {

                for (PasPrcl enrichedEvent : enrichEvents(bufferState.read())) {
                    KV<String, PasPrcl> parcelKV = KV.of(enrichedEvent.getPRCL_KEY(), enrichedEvent);
                 //   log.info("Enriched Parcel is :: " + enrichedEvent.toString());
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
            log.info("Current buffer size is ::"+ bufferState.read().spliterator().estimateSize());

            if (!bufferState.isEmpty().read()) {
                int iCounter=0;
                for (PasPrcl enrichedEvent : enrichEvents(bufferState.read())) {
                 //  log.info("Current buffer size is is::" + enrichedEvent.getPRCL_KEY());
                    KV<String, PasPrcl> parcelKV = KV.of(enrichedEvent.getPRCL_KEY(), enrichedEvent);
                    context.output(parcelKV);
                    log.info("Event Expired::"+ ++iCounter +":Enriched parcel is ::" + enrichedEvent.toString());
                 //   iCounter++;
                }
                bufferState.clear();
            }
        }

        private List<PasPrcl> enrichEvents(Iterable<PasPrcl> parcels) {
            List<PasPrcl> outPut = new ArrayList<>();
            for (PasPrcl prcl : parcels) {

                String output = this.synchClient.send(prcl.getASSESS_PRCL_ID(),
                        prcl.getSTRT_NBR_TXT() + " " + prcl.getSTRT_NM() + " " + prcl.getSTRT_TYP(),
                        prcl.getCITY_NM(),
                        prcl.getSTATE_CD(),
                        prcl.getPOSTAL_CD(),prcl.getUrl()
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




