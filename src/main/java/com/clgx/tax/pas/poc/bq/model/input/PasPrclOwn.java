package com.clgx.tax.pas.poc.bq.model.input;

import lombok.*;

import java.io.Serializable;

@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class PasPrclOwn implements Serializable {
    private String PRCL_KEY;
    private String   SOR_CD;
    private String   OWN_KEY;
    private String   OWN_TYP;
    private String   STD_FRMT_CNTRL_KEY;
    private String   CMPNY_NM;
    private String   LAST_NM;
    private String   MTRNL_LAST_NM;
    private String   FRST_NM;
    private String   MID_NM;
    private String   OWN_PFX_NM;
    private String   OWN_SFX_NM;
    private String   UNSCRB_OWN_NM;
    private String   SCND_LAST_NM;
    private String   SCND_MTRNL_LAST_NM;
    private String   SCND_FIRST_NM;
    private String   SCND_MID_NM;
    private String   OWN_PCT;
    private String   MAN_RSRCH_CD;
    private String   CNTRY_CD;
    private String   CNTY_ID;
    private String   STATE_ID;
    private String   IDNT_CHNGD_FLG;
    private String   INDT_CNFRMD_FLG;
    private String   LAST_UPDT_SRC_ID;
    private String   LAST_UPDT_TS;
    private String   LAST_UPDT_USER_ID;
}
