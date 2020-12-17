package com.clgx.tax.data.model.nts;

import lombok.*;

import java.io.Serializable;

@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class Mergedoutput implements Serializable {
    private Diablo diablo;
    private BotSchema botdata;
}
