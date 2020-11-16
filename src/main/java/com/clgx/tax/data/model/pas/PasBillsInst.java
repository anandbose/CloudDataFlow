package com.clgx.tax.data.model.pas;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class PasBillsInst implements Serializable {
    private String PRCL_KEY;
    private String SOR_CD;
    private String LIEN_KEY;
    private String BILL_KEY;
    private String PRCL_BILL_INSTL_KEY;
    private String PYMT_STAT_CD;
    private String STD_FRMT_CNTRL_KEY;
    private String CMNT_KEY;
    private String INSTL_CD;
    private String TAX_BILL_ID;
    private String IDNT_EXCPT_CD;
    private String TAX_PYMT_SRC_CD;
    private String DELQ_DT;
    private String PD_DT;
    private String TXAUTH_POST_DT;
    private String MAIL_DT;
    private String TXAUTH_TAX_SRCH_DT;
    private String TXAUTH_FILE_TYP;
    private String ORIG_SRC_CD;
    private String FILE_CRTE_DT;
    private String FILE_AQRD_DT;
    private String MAN_RSRCH_CD;
    private String DELAY_BILL_CD;
    private String TXAUTH_ZERO_AMT_CD;
    private String RDMPTN_SPCL_DOC_CD;
    private String PAYEE_ID;
    private String STAT_CD;
    private String NSF_FLG;
    private String IDNT_USER_ID;
    private String IDNT_TS;
    private String LAST_UPDT_SRC_ID;
    private String LAST_UPDT_TS;
    private String LAST_UPDT_USER_ID;
    private String TAX_SERV_AS_OF_DT;
    private String CNTY_ID;
    private String STATE_ID;
}
