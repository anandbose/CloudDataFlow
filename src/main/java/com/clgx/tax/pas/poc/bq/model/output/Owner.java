package com.clgx.tax.pas.poc.bq.model.output;

import lombok.*;

import java.io.Serializable;

@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class Owner implements Serializable {
    private String firstName;
    private String middleName;
    private String lastName;
    private String ownerType;
    private String ownerKey;
}
