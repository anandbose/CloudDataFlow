package com.clgx.tax.pas.poc.bq.model.output;

import lombok.*;

import java.io.Serializable;

@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class bqSchema implements Serializable {
    private String parcelKey;
    private String lienKey;
    private String billKey;
    private String installmentKey;
    private String amountKey;
    private String billYear;
    private String clipNumber;
    private String taxId;
    private String apnNumber;
    private String installmentId;
    private String amountType;
    private Double amount;
    private String hashKey;
}
