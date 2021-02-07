package com.clgx.tax.pas.poc.bq.model.input;

import lombok.*;

import java.io.Serializable;

@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class PasBills implements Serializable {
    private String PRCL_KEY;
    private String SOR_CD;
    private String LIEN_KEY;
    private String BILL_KEY;
    private String STD_FRMT_CNTRL_KEY;
    private String CMNT_KEY;
    private String BILL_TYP;
    private String TAX_BILL_BGN_YR;
    private String TAX_BILL_END_YR;
    private String TXAUTH_TAX_FOR_BGN_YR;
    private String TXAUTH_TAX_FOR_END_YR;
    private String TXAUTH_BILL_TAX_ID;
    private String PYMT_PLAN_ID;
    private String PYMT_PLAN_CD;
    private String PYMT_PLAN_STAT_CD;
    private String ORIG_SRC_CD;
    private String BILL_CD;
    private String BILL_STAT_CD;
    private String BILL_UID;
    private String MAN_RSRCH_CD;
    private String PYMT_PLAN_DFLT_DT;
    private String PYMT_PLAN_LAST_PAY_DT;
    private String PYMT_PLAN_UNPD_BAL_AMT;
    private String PYMT_STAT_CD;
    private String STAT_CD;
    private String BILL_EXMPT_FLG;
    private String IDNT_USER_ID;
    private String IDNT_TS;
    private String LAST_UPDT_SRC_ID;
    private String LAST_UPDT_TS;
    private String LAST_UPDT_USER_ID;
    private String TAX_SERV_AS_OF_DT;
    private String CNTY_ID;
    private String STATE_ID;
    private String PYMT_PASS_THRU;
}
