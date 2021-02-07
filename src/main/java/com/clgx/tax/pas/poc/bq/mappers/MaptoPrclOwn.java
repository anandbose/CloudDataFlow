package com.clgx.tax.pas.poc.bq.mappers;

import com.clgx.tax.pas.poc.bq.model.input.PasPrclOwn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaptoPrclOwn {
    Logger log = LoggerFactory.getLogger(MaptoPrclOwn.class);

    public PasPrclOwn maptoprcl(String[] arr)
    {
        PasPrclOwn obj = new PasPrclOwn();
        for (int i=0;i<arr.length;i++)
        {
            if(arr[i]!=null) {
                switch (i) {

                    case 0: 	obj.setPRCL_KEY(arr[i]);break;
                    case 1: 	obj.setSOR_CD(arr[i]);break;
                    case 2: 	obj.setOWN_KEY(arr[i]);break;
                    case 3: 	obj.setOWN_TYP(arr[i]);break;
                    case 4: 	obj.setSTD_FRMT_CNTRL_KEY(arr[i]);break;
                    case 5: 	obj.setCMPNY_NM(arr[i]);break;
                    case 6: 	obj.setLAST_NM(arr[i]);break;
                    case 7: 	obj.setMTRNL_LAST_NM(arr[i]);break;
                    case 8: 	obj.setFRST_NM(arr[i]);break;
                    case 9: 	obj.setMID_NM(arr[i]);break;
                    case 10:	obj.setOWN_PFX_NM(arr[i]);break;
                    case 11:	obj.setOWN_SFX_NM(arr[i]);break;
                    case 12:	obj.setUNSCRB_OWN_NM(arr[i]);break;
                    case 13:	obj.setSCND_LAST_NM(arr[i]);break;
                    case 14:	obj.setSCND_MTRNL_LAST_NM(arr[i]);break;
                    case 15:	obj.setSCND_FIRST_NM(arr[i]);break;
                    case 16:	obj.setSCND_MID_NM(arr[i]);break;
                    case 17:	obj.setOWN_PCT(arr[i]);break;
                    case 18:	obj.setMAN_RSRCH_CD(arr[i]);break;
                    case 19:	obj.setCNTRY_CD(arr[i]);break;
                    case 20:	obj.setCNTY_ID(arr[i]);break;
                    case 21:	obj.setSTATE_ID(arr[i]);break;
                    case 22:	obj.setIDNT_CHNGD_FLG(arr[i]);break;
                    case 23:	obj.setINDT_CNFRMD_FLG(arr[i]);break;
                    case 24:	obj.setLAST_UPDT_SRC_ID(arr[i]);break;
                    case 25:	obj.setLAST_UPDT_TS(arr[i]);break;
                    case 26:	obj.setLAST_UPDT_USER_ID(arr[i]);break;
                }
            }
        }

        log.info("--ParcelOwner--"+obj.toString());
        return  obj;
    }
}
