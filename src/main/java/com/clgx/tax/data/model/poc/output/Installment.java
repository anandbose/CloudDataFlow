package com.clgx.tax.data.model.poc.output;

import lombok.*;

import java.io.Serializable;
import java.util.Date;
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

    private List<Amount> amounts;
    private Double amountTotals;
}
