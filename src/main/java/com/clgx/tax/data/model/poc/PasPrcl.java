package com.clgx.tax.data.model.poc;

import lombok.*;

import java.io.Serializable;

@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class PasPrcl implements Serializable,Cloneable {

    private String  PRCL_KEY;
    private String  SOR_CD;
    private String  STD_FRMT_CNTRL_KEY;
    private String  PRCL_CMNT_KEY;
    private String  PRCL_TYP;
    private String  STAT_CD;
    private String  STRT_PRE_DIR_CD;
    private String  STRT_NBR_TXT;
    private String  STRT_NBR_END_TXT;
    private String  STRT_NBR_FRCTN_TXT;
    private String  STRT_NM;
    private String  STRT_TYP;
    private String  STRT_POST_DIR_CD;
    private String  UNIT_TYP;
    private String  UNIT_NBR_TXT;
    private String  UNIT_NBR_END_TXT;
    private String  PO_BOX_NBR_TXT;
    private String  BOX_LOC_TXT;
    private String  RR_CD;
    private String  RR_NBR;
    private String  NGHBRHD_NM;
    private String  CITY_NM;
    private String  CNTY_NM;
    private String  POSTAL_CD;
    private String  CNTRY_CD;
    private String  STATE_CD;
    private String  STATE_SUB_CD;
    private String  GEO_SRC_CD;
    private String  FIPS_STATE_ID;
    private String  FIPS_CNTY_ID;
    private String  PRCL_USE_CD;
    private String  SRVY_ID;
    private String  PRCL_SCND_USE_NM;
    private String  BLDG_CLS_CD;
    private String  CNSTCT_YR;
    private String  LAND_TYP_CD;
    private String  LAND_USE_NM;
    private String  PRPTY_SOLD_DT;
    private String  LAST_IMPRV_YR;
    private String  OWN_ACQ_DT;
    private String  ESMT_TYP_CD;
    private String  PRPTY_ZONE_CD;
    private String  CENSUS_TRACT_CD;
    private String  FLOOD_ZONE_CD;
    private String  NO_TXAUTH_FLG;
    private String  ORIG_SRC_CD;
    private String  MOBL_HOME_VIN_ID;
    private String  ASSESS_PRCL_ID;
    private String  ASSESS_YR;
    private String  FILE_CRTE_DT;
    private String  NON_ADV_PRPTY_DESC;
    private String  NON_ADV_PRPTY_VALUE_AMT;
    private String  MAN_RSRCH_CD;
    private String  UAT_ADDR_TAG_ID;
    private String  PRCL_USE_NM;
    private String  PRCL_SCND_USE_CD;
    private String  FULL_EXMPT_FLG;
    private String  TAX_AREA;
    private String  LAND_TYP_NM;
    private String  BLDG_CLS_NM;
    private String  NGHBRHD_CD;
    private String  GEO_CD_LATTD;
    private String  GEO_CD_LONGTD;
    private String  MAP_GRID_LOC_TXT;
    private String  UNSCRB_SITUS_ADDR_TXT;
    private String  UNSCRB_SITUS_ADDR_LN2_TXT;
    private String  CNTY_ID;
    private String  STATE_ID;
    private String  IDNT_CHNGD_FLG;
    private String  IDNT_CNFRMD_FLG;
    private String  IDNT_ID;
    private String  LAST_UPDT_SRC_ID;
    private String  LAST_UPDT_TS;
    private String  LAST_UPDT_USER_ID;
    private String  TAX_SERV_AS_OF_DT;
    private String  clipNumber;

    public Object clone()throws CloneNotSupportedException{
        return (PasPrcl)super.clone();
    }

    public String createOutput()
    {
        return this.PRCL_KEY+"|"+
                this.SOR_CD+"|"+
                this.STD_FRMT_CNTRL_KEY+"|"+
                this.PRCL_CMNT_KEY+"|"+
                this.PRCL_TYP+"|"+
                this.STAT_CD+"|"+
                this.STRT_PRE_DIR_CD+"|"+
                this.STRT_NBR_TXT+"|"+
                this.STRT_NBR_END_TXT+"|"+
                this.STRT_NBR_FRCTN_TXT+"|"+
                this.STRT_NM+"|"+
                this.STRT_TYP+"|"+
                this.STRT_POST_DIR_CD+"|"+
                this.UNIT_TYP+"|"+
                this.UNIT_NBR_TXT+"|"+
                this.UNIT_NBR_END_TXT+"|"+
                this.PO_BOX_NBR_TXT+"|"+
                this.BOX_LOC_TXT+"|"+
                this.RR_CD+"|"+
                this.RR_NBR+"|"+
                this.NGHBRHD_NM+"|"+
                this.CITY_NM+"|"+
                this.CNTY_NM+"|"+
                this.POSTAL_CD+"|"+
                this.CNTRY_CD+"|"+
                this.STATE_CD+"|"+
                this.STATE_SUB_CD+"|"+
                this.GEO_SRC_CD+"|"+
                this.FIPS_STATE_ID+"|"+
                this.FIPS_CNTY_ID+"|"+
                this.PRCL_USE_CD+"|"+
                this.SRVY_ID+"|"+
                this.PRCL_SCND_USE_NM+"|"+
                this.BLDG_CLS_CD+"|"+
                this.CNSTCT_YR+"|"+
                this.LAND_TYP_CD+"|"+
                this.LAND_USE_NM+"|"+
                this.PRPTY_SOLD_DT+"|"+
                this.LAST_IMPRV_YR+"|"+
                this.OWN_ACQ_DT+"|"+
                this.ESMT_TYP_CD+"|"+
                this.PRPTY_ZONE_CD+"|"+
                this.CENSUS_TRACT_CD+"|"+
                this.FLOOD_ZONE_CD+"|"+
                this.NO_TXAUTH_FLG+"|"+
                this.ORIG_SRC_CD+"|"+
                this.MOBL_HOME_VIN_ID+"|"+
                this.ASSESS_PRCL_ID+"|"+
                this.ASSESS_YR+"|"+
                this.FILE_CRTE_DT+"|"+
                this.NON_ADV_PRPTY_DESC+"|"+
                this.NON_ADV_PRPTY_VALUE_AMT+"|"+
                this.MAN_RSRCH_CD+"|"+
                this.UAT_ADDR_TAG_ID+"|"+
                this.PRCL_USE_NM+"|"+
                this.PRCL_SCND_USE_CD+"|"+
                this.FULL_EXMPT_FLG+"|"+
                this.TAX_AREA+"|"+
                this.LAND_TYP_NM+"|"+
                this.BLDG_CLS_NM+"|"+
                this.NGHBRHD_CD+"|"+
                this.GEO_CD_LATTD+"|"+
                this.GEO_CD_LONGTD+"|"+
                this.MAP_GRID_LOC_TXT+"|"+
                this.UNSCRB_SITUS_ADDR_TXT+"|"+
                this.UNSCRB_SITUS_ADDR_LN2_TXT+"|"+
                this.CNTY_ID+"|"+
                this.STATE_ID+"|"+
                this.IDNT_CHNGD_FLG+"|"+
                this.IDNT_CNFRMD_FLG+"|"+
                this.IDNT_ID+"|"+
                this.LAST_UPDT_SRC_ID+"|"+
                this.LAST_UPDT_TS+"|"+
                this.LAST_UPDT_USER_ID+"|"+
                this.TAX_SERV_AS_OF_DT+"|"+
                this.clipNumber;
    }

}
