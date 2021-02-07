package com.clgx.tax.pas.poc.bq.model.output;

import lombok.*;

import java.io.Serializable;
import java.util.List;

@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class Installment implements Serializable {
    private String installmentUniqueKey;
    private String installmentID;
    private String installmentType;
  //  private Date installmentBeginDate;
 //   private Date installmentEndDate;

    private List<com.clgx.tax.data.model.poc.output.Amount> amounts;
    private List<com.clgx.tax.data.model.poc.output.bqSchema> bigQueryRecs;
    private Double amountTotals;
}
