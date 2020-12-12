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
public class Owner implements Serializable {
    private String firstName;
    private String middleName;
    private String lastName;
    private String ownerType;
    private String ownerKey;
}
