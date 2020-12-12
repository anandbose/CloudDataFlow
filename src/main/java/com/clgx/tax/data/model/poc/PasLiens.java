package com.clgx.tax.data.model.poc;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Data
@Getter
@Setter
@NoArgsConstructor

public class PasLiens implements Serializable {
 private String PRCL_KEY;
 private String  SOR_CD;
 private String  LIEN_KEY;
 private String  TAX_ID;
 private String  TXAUTH_ID;
 private String  CNTRY_CD;
 private String  STATE_ID;
 private String  CNTY_ID;
 private String  STAT_CD;
 private String  STD_FRMT_CNTRL_KEY;
 private String  PRCL_CMNT_KEY;
 private String  TXAUTH_FILE_TYP;
 private String  LIEN_TYP;
 private String  DSPLY_TAX_ID_FLG;
 private String  MUNI_CD;
 private String  SCHL_DSTRC_CD;
 private String  PEND_APRTN_FLG;
 private String  PEND_APRTN_DT;
 private String  ORIG_SRC_CD;
 private String  FILLER1;
 private String  LGCY_PRIM_TXAUTH_ID;
 private String  LGCY_PRIM_TAX_ID;
 private String  PREV_TAX_ID;
 private String  PRIOR_YR_DELQ_FLG;
 private String  MAN_RSRCH_CD;
 private String  IDNT_USER_ID;
 private String  IDNT_CHNGD_FLG;
 private String  IDNT_CNFRMD_FLG;
 private String  IDNT_METH_CD;
 private String  IDNT_ID;
 private String  IDNT_TS;
 private String  LAST_UPDT_USER_ID;
 private String  LAST_UPDT_SRC_ID;
 private String  LAST_UPDT_TS;
 private String  TAX_SERV_AS_OF_DT;
 private String  PEND_APRTN_RSN_CD ;
}
