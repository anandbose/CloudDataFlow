package com.clgx.tax.data.model.poc.output;

import lombok.*;

import java.io.Serializable;

@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class Address implements Serializable {
    private  String unitNumber;
    private String streetDirection;
    private String streetAddress;
    private String city;
    private String postalCode;
    private String countyName;
}
