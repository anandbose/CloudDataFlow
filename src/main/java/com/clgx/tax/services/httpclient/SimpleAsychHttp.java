package com.clgx.tax.services.httpclient;

import io.netty.handler.codec.http.HttpMethod;
import org.asynchttpclient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.asynchttpclient.Dsl.*;


public class SimpleAsychHttp {
    AsyncHttpClient Client;
    Logger log = LoggerFactory.getLogger(SimpleAsychHttp.class);
    public SimpleAsychHttp()
    {
        AsyncHttpClientConfig config =
                Dsl.config()
                        .setConnectTimeout(1000)
                        .setRequestTimeout(5000)
                        .setKeepAlive(true)
                        .build();
        Client =  asyncHttpClient(config);
    }


    public String send(String apn,
                       String  address,
                       String city,
                       String state,
                       String zipCode) {
        String lclZipcode = (zipCode!=null && zipCode.length()>=5) ? zipCode : "";
        Request req = new RequestBuilder()
                .setUrl("https://uat-west-clp-coreapi-clip-lookup.apps.uat.pcfusw1stg.solutions.corelogic.com/search/apn")
                .setMethod(HttpMethod.GET.name())
                .addHeader("x-api-key","xAbaGhS2orRCICWSAYiKXfBUHBrY1S90")
                // .setH
                //.setHeader("Authorization", "XXXX")
                .addQueryParam("legacyCountySource","true")
                .addQueryParam("bestMatch","true")
                .addQueryParam("googleFallback","false")
                .addQueryParam("apn",apn)
                .addQueryParam("address",address)
                .addQueryParam("city",city)
                .addQueryParam("state",state)
                .addQueryParam("lclZipcode",zipCode.substring(0,4))

                //.setBody(input)
                .build();

        ListenableFuture<Response> synchResponse = Client.prepareRequest(req).execute();
        String finalResponse;
        try {

            Response res = synchResponse.get(10, TimeUnit.SECONDS);
            Client.close();
            return new String(res.getResponseBodyAsBytes());
        }
        catch(Exception ex)
        {
           // ex.printStackTrace();
            log.error("Error getting response ::"+ex.getMessage());

            return "";

        }
        finally {
            try {
                Client.close();
            }catch (Exception ex){log.error("Error closing request  ::"+ex.getMessage());}
        }


    }
}
