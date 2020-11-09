package com.clgx.tax.data.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.Date;


@ToString
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class HashRec implements Serializable {

    private  String hashKey;

    private String tableName;
    private Date created;
    private Date updated;


    private  String billdata;

    protected void onCreate()
    {
        created = new Date();
    }
    protected void onUpdate()
    {
        updated = new Date();
    }

}
