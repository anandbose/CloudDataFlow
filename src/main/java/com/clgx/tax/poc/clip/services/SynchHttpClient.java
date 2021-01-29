package com.clgx.tax.poc.clip.services;


import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SynchHttpClient {
    Logger log = LoggerFactory.getLogger(SynchHttpClient.class);
    public String send(String apn,
                       String  address,
                       String city,
                       String state,
                       String zipCode, String url, String apiKey) {
        CloseableHttpClient client = HttpClients.createDefault();
        List<NameValuePair> obj= new ArrayList<>();
        String result="";
        try
        {

           // HttpRequestBase request = new HttpGet("https://uat-west-clp-coreapi-clip-lookup.apps.uat.pcfusw1stg.solutions.corelogic.com/search/apn");
           // URIBuilder builder = new URIBuilder("https://uat-west-clp-coreapi-clip-lookup.apps.uat.pcfusw1stg.solutions.corelogic.com/search/apn");
            log.info("The url to call is ::"+url);
            URIBuilder builder = new URIBuilder(url);
            obj.add(new BasicNameValuePair("legacyCountySource","true"));
            obj.add(new BasicNameValuePair("bestMatch","true"));
            obj.add(new BasicNameValuePair("googleFallback","false"));
            obj.add(new BasicNameValuePair("apn",apn));
            obj.add(new BasicNameValuePair("address",address));
            obj.add(new BasicNameValuePair("city",city));
            obj.add(new BasicNameValuePair("state",state));
            if (zipCode != null && zipCode.length() >=5)
              obj.add(new BasicNameValuePair("zipCode",zipCode.substring(0,4)));

            builder.addParameters(obj);
            HttpGet request = new HttpGet(builder.build());
            // add request headers
            request.addHeader("x-api-key",apiKey);
           // request.setP

           // HttpRequestBase request = new H
            CloseableHttpResponse response = client.execute(request);
            try {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    // return it as a String
                    result = EntityUtils.toString(entity);
                    System.out.println(result);

                }

            }
            catch (Exception ex)
            {
                log.error("Error getting response ::"+ex.getMessage());
                ex.printStackTrace();

            }
            finally {
                response.close();
            }



        }
        catch(Exception ex)
        {
            log.error("Error with processing "+ex.getMessage());
           ex.printStackTrace();

        }
        finally{
            return result;
        }

       // return null;
    }
}
