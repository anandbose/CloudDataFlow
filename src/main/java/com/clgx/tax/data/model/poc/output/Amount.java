package com.clgx.tax.data.model.poc.output;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Data
@Getter
@Setter
@NoArgsConstructor
public class Amount implements Serializable {
    private String amountType;
    private Double amount;
}
