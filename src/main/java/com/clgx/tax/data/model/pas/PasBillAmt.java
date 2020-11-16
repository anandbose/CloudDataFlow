package com.clgx.tax.data.model.pas;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class PasBillAmt implements Serializable {

    private String PRCL_KEY;
    private String SOR_CD;
    private String LIEN_KEY;
    private String BILL_KEY;
    private String PRCL_BILL_INSTL_KEY;
    private String PRCL_BILL_AMT_KEY;
    private String CMNT_KEY;
    private String BILL_AMT_TYP;
    private String CRNCY_TYP;
    private Double BILL_AMT;
    private String GOOD_THRU_DT;
    private String ORIG_SRC_CD;
    private String MAN_RSRCH_CD;
    private String STAT_CD;
    private String STD_FRMT_CNTRL_KEY;
    private String IDNT_USER_ID;
    private String IDNT_TS;
    private String LAST_UPDT_SRC_ID;
    private String LAST_UPDT_TS;
    private String LAST_UPDT_USER_ID;
    private String TAX_SERV_AS_OF_DT;
    private String CNTY_ID;
    private String STATE_ID;
    //merge from install code
    private String INSTL_CD;
}
