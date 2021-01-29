package com.clgx.tax.poc.clip.model.output;

import lombok.*;

import java.io.Serializable;
import java.util.List;

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
    private List<Owner> owners;
    private List<Installment> installments;

    public Object clone() throws
            CloneNotSupportedException
    {
        return super.clone();
    }



}
