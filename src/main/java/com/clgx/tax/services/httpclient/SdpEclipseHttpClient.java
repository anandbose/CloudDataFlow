package com.clgx.tax.services.httpclient;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.http.HttpHeaders;
import org.asynchttpclient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

public class SdpEclipseHttpClient {
 AsyncHttpClient httpClient;
 static final Logger log = LoggerFactory.getLogger(SdpEclipseHttpClient.class);

 public SdpEclipseHttpClient()
 {
     DefaultAsyncHttpClientConfig.Builder configBuilder = config()
             .setConnectTimeout(90000)
             .setRequestTimeout(90000)
             .setMaxRequestRetry(3)
             .setMaxConnectionsPerHost(100)
             .setReadTimeout(90000);



     ThreadPoolTaskExecutor threadFactory = new ThreadPoolTaskExecutor();
     threadFactory.setMaxPoolSize(32);
     threadFactory.setKeepAliveSeconds(20);
     configBuilder.setThreadFactory(threadFactory);

     AsyncHttpClientConfig clientConfig = configBuilder.build();
     httpClient = asyncHttpClient(clientConfig);

 }

 public String send(String apn,
                    String  address,
                    String city,
                    String state,
                    String zipCode){


  Request request = new RequestBuilder()
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
          .addQueryParam("zipCode",zipCode.substring(0,4))

          //.setBody(input)
          .build();
  //log.info("")
      ListenableFuture<Response> responseListenableFuture = httpClient.executeRequest(request, new AsyncCompletionHandler<Response>() {
           @Override
            public Response onCompleted(Response response) throws Exception {
             System.out.println("Response is ::"+new String(response.getResponseBodyAsBytes()));
             return response;
            }
      });
      String returnString;
      try
      {
       returnString = new String(responseListenableFuture.get(10, TimeUnit.SECONDS).getResponseBodyAsBytes());
       return returnString;
      }
      catch (Exception ex)
      {
       ex.printStackTrace();
       return null;
      }

 }


}
