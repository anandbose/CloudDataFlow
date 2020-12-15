package com.clgx.tax.data.model.poc.output;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Data
@Getter
@Setter
@NoArgsConstructor
public class Parcel implements Serializable {
    private String prclKey;
    private String billYear;

    private Address address;
    private String taxId;
    private String stateCounty;
    private List<Owner> owners;
    private List<Installment> installments;




}
