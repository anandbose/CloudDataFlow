package com.clgx.tax.pas.poc.bq.model.output;

import lombok.*;

import java.io.Serializable;
import java.util.List;

//@DefaultCoder(AvroCoder.class)
@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class Parcel implements Serializable,Cloneable {
    private String prclKey;
    private String billYear;
    private String clipNumber;
    private Address address;
    private String taxId;
    private String stateCounty;
    private String apnNumber;
    private List<Owner> owners;
    private List<Installment> installments;
    private List<String> taxIds;

    public Object clone() throws
            CloneNotSupportedException
    {
        return super.clone();
    }



}
