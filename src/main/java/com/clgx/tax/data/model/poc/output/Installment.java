package com.clgx.tax.data.model.poc.output;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

@Data
@Getter
@Setter
@NoArgsConstructor
public class Installment implements Serializable {
    private String installmentID;
    private String installmentType;
  //  private Date installmentBeginDate;
 //   private Date installmentEndDate;

    private List<Amount> amounts;
    private Double amountTotals;
}
