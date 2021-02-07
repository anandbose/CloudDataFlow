package com.clgx.tax.pas.poc.bq.model.output;

import lombok.*;

import java.io.Serializable;
import java.util.List;

@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class CombinedRecordsets implements Serializable {
    private  Parcel prcl ;
    private List<OutputByInstallment> installments;
}
