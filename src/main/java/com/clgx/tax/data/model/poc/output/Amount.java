package com.clgx.tax.data.model.poc.output;

import lombok.*;

import java.io.Serializable;

@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class Amount implements Serializable {
    private String amountType;
    private Double amount;
}
