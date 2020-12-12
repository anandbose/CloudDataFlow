package com.clgx.tax.mappers.poc;

import com.clgx.tax.data.model.poc.PasPrcl;
import com.clgx.tax.data.model.poc.PasPrclOwn;

public class MaptoPrclOwn {

    public PasPrclOwn maptoprcl(String[] arr)
    {
        PasPrclOwn obj = new PasPrclOwn();
        obj.setPRCL_KEY(arr[0]);
        obj.setSOR_CD(arr[1]);
        obj.setOWN_KEY(arr[2]);
        obj.setOWN_TYP(arr[3]);
        obj.setSTD_FRMT_CNTRL_KEY(arr[4]);
        obj.setCMPNY_NM(arr[5]);
        obj.setLAST_NM(arr[6]);
        obj.setMTRNL_LAST_NM(arr[7]);
        obj.setFRST_NM(arr[8]);
        obj.setMID_NM(arr[9]);
        obj.setOWN_PFX_NM(arr[10]);
        obj.setOWN_SFX_NM(arr[11]);
        obj.setUNSCRB_OWN_NM(arr[12]);
        obj.setSCND_LAST_NM(arr[13]);
        obj.setSCND_MTRNL_LAST_NM(arr[14]);
        obj.setSCND_FIRST_NM(arr[15]);
        obj.setSCND_MID_NM(arr[16]);
        obj.setOWN_PCT(arr[17]);
        obj.setMAN_RSRCH_CD(arr[18]);
        obj.setCNTRY_CD(arr[19]);
        obj.setCNTY_ID(arr[20]);
        obj.setSTATE_ID(arr[21]);
        obj.setIDNT_CHNGD_FLG(arr[22]);
        obj.setINDT_CNFRMD_FLG(arr[23]);
        obj.setLAST_UPDT_SRC_ID(arr[24]);
        obj.setLAST_UPDT_TS(arr[25]);
        obj.setLAST_UPDT_USER_ID(arr[26]);
        return  obj;
    }
}
