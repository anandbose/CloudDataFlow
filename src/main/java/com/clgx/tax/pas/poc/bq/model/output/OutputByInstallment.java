package com.clgx.tax.pas.poc.bq.model.output;

import lombok.*;

import java.io.Serializable;
import java.util.List;

@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class OutputByInstallment implements Serializable {


    private String prclKey;
    private Address address;
    private Double amountTotals;
    private String stateCounty;
    private String billYear;
    private String installmentID;
    private String installmentType;
    //  private Date installmentBeginDate;
    //   private Date installmentEndDate;
    private String installmentUniqueKey;
    private List<Owner> owners;
    private List<Amount> amounts;




}
