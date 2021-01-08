package com.clgx.tax.mappers.poc;

import com.clgx.tax.data.model.poc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaptoPasPrcl {
    Logger log = LoggerFactory.getLogger(MaptoPasPrcl.class);
    public PasPrcl maptoprcl(String[] arr)
    {
        PasPrcl obj = new PasPrcl();
        for (int i=0;i<arr.length;i++)
        {
            if(arr[i]!=null) {
                switch (i) {

                    case 0:obj.setPRCL_KEY(arr[i]);break;
                    case 1:obj.setSOR_CD(arr[i]);break;
                    case 2:obj.setSTD_FRMT_CNTRL_KEY(arr[i]);break;
                    case 3:obj.setPRCL_CMNT_KEY(arr[i]);break;
                    case 4:obj.setPRCL_TYP(arr[i]);break;
                    case 5:obj.setSTAT_CD(arr[i]);break;
                    case 6:obj.setSTRT_PRE_DIR_CD(arr[i]);break;
                    case 7:obj.setSTRT_NBR_TXT(arr[i]);break;
                    case 8:obj.setSTRT_NBR_END_TXT(arr[i]);break;
                    case 9:obj.setSTRT_NBR_FRCTN_TXT(arr[i]);break;
                    case 10:obj.setSTRT_NM(arr[i]);break;
                    case 11:obj.setSTRT_TYP(arr[i]);break;
                    case 12:obj.setSTRT_POST_DIR_CD(arr[i]);break;
                    case 13:obj.setUNIT_TYP(arr[i]);break;
                    case 14:obj.setUNIT_NBR_TXT(arr[i]);break;
                    case 15:obj.setUNIT_NBR_END_TXT(arr[i]);break;
                    case 16:obj.setPO_BOX_NBR_TXT(arr[i]);break;
                    case 17:obj.setBOX_LOC_TXT(arr[i]);break;
                    case 18:obj.setRR_CD(arr[i]);break;
                    case 19:obj.setRR_NBR(arr[i]);break;
                    case 20:obj.setNGHBRHD_NM(arr[i]);break;
                    case 21:obj.setCITY_NM(arr[i]);break;
                    case 22:obj.setCNTY_NM(arr[i]);break;
                    case 23:obj.setPOSTAL_CD(arr[i]);break;
                    case 24:obj.setCNTRY_CD(arr[i]);break;
                    case 25:obj.setSTATE_CD(arr[i]);break;
                    case 26:obj.setSTATE_SUB_CD(arr[i]);break;
                    case 27:obj.setGEO_SRC_CD(arr[i]);break;
                    case 28:obj.setFIPS_STATE_ID(arr[i]);break;
                    case 29:obj.setFIPS_CNTY_ID(arr[i]);break;
                    case 30:obj.setPRCL_USE_CD(arr[i]);break;
                    case 31:obj.setSRVY_ID(arr[i]);break;
                    case 32:obj.setPRCL_SCND_USE_NM(arr[i]);break;
                    case 33:obj.setBLDG_CLS_CD(arr[i]);break;
                    case 34:obj.setCNSTCT_YR(arr[i]);break;
                    case 35:obj.setLAND_TYP_CD(arr[i]);break;
                    case 36:obj.setLAND_USE_NM(arr[i]);break;
                    case 37:obj.setPRPTY_SOLD_DT(arr[i]);break;
                    case 38:obj.setLAST_IMPRV_YR(arr[i]);break;
                    case 39:obj.setOWN_ACQ_DT(arr[i]);break;
                    case 40:obj.setESMT_TYP_CD(arr[i]);break;
                    case 41:obj.setPRPTY_ZONE_CD(arr[i]);break;
                    case 42:obj.setCENSUS_TRACT_CD(arr[i]);break;
                    case 43:obj.setFLOOD_ZONE_CD(arr[i]);break;
                    case 44:obj.setNO_TXAUTH_FLG(arr[i]);break;
                    case 45:obj.setORIG_SRC_CD(arr[i]);break;
                    case 46:obj.setMOBL_HOME_VIN_ID(arr[i]);break;
                    case 47:obj.setASSESS_PRCL_ID(arr[i]);break;
                    case 48:obj.setASSESS_YR(arr[i]);break;
                    case 49:obj.setFILE_CRTE_DT(arr[i]);break;
                    case 50:obj.setNON_ADV_PRPTY_DESC(arr[i]);break;
                    case 51:obj.setNON_ADV_PRPTY_VALUE_AMT(arr[i]);break;
                    case 52:obj.setMAN_RSRCH_CD(arr[i]);break;
                    case 53:obj.setUAT_ADDR_TAG_ID(arr[i]);break;
                    case 54:obj.setPRCL_USE_NM(arr[i]);break;
                    case 55:obj.setPRCL_SCND_USE_CD(arr[i]);break;
                    case 56:obj.setFULL_EXMPT_FLG(arr[i]);break;
                    case 57:obj.setTAX_AREA(arr[i]);break;
                    case 58:obj.setLAND_TYP_NM(arr[i]);break;
                    case 59:obj.setBLDG_CLS_NM(arr[i]);break;
                    case 60:obj.setNGHBRHD_CD(arr[i]);break;
                    case 61:obj.setGEO_CD_LATTD(arr[i]);break;
                    case 62:obj.setGEO_CD_LONGTD(arr[i]);break;
                    case 63:obj.setMAP_GRID_LOC_TXT(arr[i]);break;
                    case 64:obj.setUNSCRB_SITUS_ADDR_TXT(arr[i]);break;
                    case 65:obj.setUNSCRB_SITUS_ADDR_LN2_TXT(arr[i]);break;
                    case 66:obj.setCNTY_ID(arr[i]);break;
                    case 67:obj.setSTATE_ID(arr[i]);break;
                    case 68:obj.setIDNT_CHNGD_FLG(arr[i]);break;
                    case 69:obj.setIDNT_CNFRMD_FLG(arr[i]);break;
                    case 70:obj.setIDNT_ID(arr[i]);break;
                    case 71:obj.setLAST_UPDT_SRC_ID(arr[i]);break;
                    case 72:obj.setLAST_UPDT_TS(arr[i]);break;
                    case 73:obj.setLAST_UPDT_USER_ID(arr[i]);break;
                    case 74:obj.setTAX_SERV_AS_OF_DT(arr[i]);break;


                    case 75:obj.setClipNumber(arr[i]);break;

                }
            }
        }

     //   obj.setTAX_SERV_AS_OF_DT(arr[74]);
        log.info("--PAS PARCEL--"+obj.toString());
        return obj;
    }

    public PasLiens mapToLiens(String[] arr)
    {
        PasLiens obj = new PasLiens();
        for (int i=0;i<arr.length;i++)
        {
            if(arr[i]!=null) {
                switch (i) {

                    case 0: 	obj.setPRCL_KEY(arr[i]);
                    case 1: 	obj.setSOR_CD(arr[i]);
                    case 2: 	obj.setLIEN_KEY(arr[i]);
                    case 3: 	obj.setTAX_ID(arr[i]);
                    case 4: 	obj.setTXAUTH_ID(arr[i]);
                    case 5: 	obj.setCNTRY_CD(arr[i]);
                    case 6: 	obj.setSTATE_ID(arr[i]);
                    case 7: 	obj.setCNTY_ID(arr[i]);
                    case 8: 	obj.setSTAT_CD(arr[i]);
                    case 9: 	obj.setSTD_FRMT_CNTRL_KEY(arr[i]);
                    case 10:	obj.setPRCL_CMNT_KEY(arr[i]);
                    case 11:	obj.setTXAUTH_FILE_TYP(arr[i]);
                    case 12:	obj.setLIEN_TYP(arr[i]);
                    case 13:	obj.setDSPLY_TAX_ID_FLG(arr[i]);
                    case 14:	obj.setMUNI_CD(arr[i]);
                    case 15:	obj.setSCHL_DSTRC_CD(arr[i]);
                    case 16:	obj.setPEND_APRTN_FLG(arr[i]);
                    case 17:	obj.setPEND_APRTN_DT(arr[i]);
                    case 18:	obj.setORIG_SRC_CD(arr[i]);
                    case 19:	obj.setFILLER1(arr[i]);
                    case 20:	obj.setLGCY_PRIM_TXAUTH_ID(arr[i]);
                    case 21:	obj.setLGCY_PRIM_TAX_ID(arr[i]);
                    case 22:	obj.setPREV_TAX_ID(arr[i]);
                    case 23:	obj.setPRIOR_YR_DELQ_FLG(arr[i]);
                    case 24:	obj.setMAN_RSRCH_CD(arr[i]);
                    case 25:	obj.setIDNT_USER_ID(arr[i]);
                    case 26:	obj.setIDNT_CHNGD_FLG(arr[i]);
                    case 27:	obj.setIDNT_CNFRMD_FLG(arr[i]);
                    case 28:	obj.setIDNT_METH_CD(arr[i]);
                    case 29:	obj.setIDNT_ID(arr[i]);
                    case 30:	obj.setIDNT_TS(arr[i]);
                    case 31:	obj.setLAST_UPDT_USER_ID(arr[i]);
                    case 32:	obj.setLAST_UPDT_SRC_ID(arr[i]);
                    case 33:	obj.setLAST_UPDT_TS(arr[i]);
                    case 34:	obj.setTAX_SERV_AS_OF_DT(arr[i]);
                    case 35:	obj.setPEND_APRTN_RSN_CD (arr[i]);
                }
            }
        }




        log.info("--PAS Liens--"+obj.toString());

        return obj;
    }


    public PasBillsInst mapToBillsInst(String[] args)
    {
        PasBillsInst obj = new PasBillsInst();

        for (int i=0;i<args.length;i++) {
            if (args[i] != null) {
                switch (i) {
                    case 0: 	obj.setPRCL_KEY(args[i]);break;
                    case 1: 	obj.setSOR_CD(args[i]);break;
                    case 2: 	obj.setLIEN_KEY(args[i]);break;
                    case 3: 	obj.setBILL_KEY(args[i]);break;
                    case 4: 	obj.setPRCL_BILL_INSTL_KEY(args[i]);break;
                    case 5: 	obj.setPYMT_STAT_CD(args[i]);break;
                    case 6: 	obj.setSTD_FRMT_CNTRL_KEY(args[i]);break;
                    case 7: 	obj.setCMNT_KEY(args[i]);break;
                    case 8: 	obj.setINSTL_CD(args[i]);break;
                    case 9: 	obj.setTAX_BILL_ID(args[i]);break;
                    case 10:	obj.setIDNT_EXCPT_CD(args[i]);break;
                    case 11:	obj.setTAX_PYMT_SRC_CD(args[i]);break;
                    case 12:	obj.setDELQ_DT(args[i]);break;
                    case 13:	obj.setPD_DT(args[i]);break;
                    case 14:	obj.setTXAUTH_POST_DT(args[i]);break;
                    case 15:	obj.setMAIL_DT(args[i]);break;
                    case 16:	obj.setTXAUTH_TAX_SRCH_DT(args[i]);break;
                    case 17:	obj.setTXAUTH_FILE_TYP(args[i]);break;
                    case 18:	obj.setORIG_SRC_CD(args[i]);break;
                    case 19:	obj.setFILE_CRTE_DT(args[i]);break;
                    case 20:	obj.setFILE_AQRD_DT(args[i]);break;
                    case 21:	obj.setMAN_RSRCH_CD(args[i]);break;
                    case 22:	obj.setDELAY_BILL_CD(args[i]);break;
                    case 23:	obj.setTXAUTH_ZERO_AMT_CD(args[i]);break;
                    case 24:	obj.setRDMPTN_SPCL_DOC_CD(args[i]);break;
                    case 25:	obj.setPAYEE_ID(args[i]);break;
                    case 26:	obj.setSTAT_CD(args[i]);break;
                    case 27: 	obj.setNSF_FLG(args[i]);break;
                    case 28:	obj.setIDNT_USER_ID(args[i]);break;
                    case 29:	obj.setIDNT_TS(args[i]);break;
                    case 30:	obj.setLAST_UPDT_SRC_ID(args[i]);break;
                    case 31:	obj.setLAST_UPDT_TS(args[i]);break;
                    case 32:	obj.setLAST_UPDT_USER_ID(args[i]);break;
                    case 33:	obj.setTAX_SERV_AS_OF_DT(args[i]);break;
                    case 34:	obj.setCNTY_ID(args[i]);break;
                    case 35:	obj.setSTATE_ID(args[i]);break;
                }
            }
        }

        log.info("--PAS Bill Installments--"+obj.toString());

        return obj;
    }

    public PasBills mapToBills(String[] args)
    {
        PasBills obj = new PasBills();

        for (int i=0;i<args.length;i++) {
            if (args[i] != null) {
                switch (i) {
                    case 0: 	obj.setPRCL_KEY(args[i]);break;
                    case 1: 	obj.setSOR_CD(args[i]);break;
                    case 2: 	obj.setLIEN_KEY(args[i]);break;
                    case 3: 	obj.setBILL_KEY(args[i]);break;
                    case 4: 	obj.setSTD_FRMT_CNTRL_KEY(args[i]);break;
                    case 5: 	obj.setCMNT_KEY(args[i]);break;
                    case 6: 	obj.setBILL_TYP(args[i]);break;
                    case 7: 	obj.setTAX_BILL_BGN_YR(args[i]);break;
                    case 8: 	obj.setTAX_BILL_END_YR(args[i]);break;
                    case 9: 	obj.setTXAUTH_TAX_FOR_BGN_YR(args[i]);break;
                    case 10:	obj.setTXAUTH_TAX_FOR_END_YR(args[i]);break;
                    case 11:	obj.setTXAUTH_BILL_TAX_ID(args[i]);break;
                    case 12:	obj.setPYMT_PLAN_ID(args[i]);break;
                    case 13:	obj.setPYMT_PLAN_CD(args[i]);break;
                    case 14:	obj.setPYMT_PLAN_STAT_CD(args[i]);break;
                    case 15:	obj.setORIG_SRC_CD(args[i]);break;
                    case 16:	obj.setBILL_CD(args[i]);break;
                    case 17:	obj.setBILL_STAT_CD(args[i]);break;
                    case 18:	obj.setBILL_UID(args[i]);break;
                    case 19:	obj.setMAN_RSRCH_CD(args[i]);break;
                    case 20:	obj.setPYMT_PLAN_DFLT_DT(args[i]);break;
                    case 21:	obj.setPYMT_PLAN_LAST_PAY_DT(args[i]);break;
                    case 22:	obj.setPYMT_PLAN_UNPD_BAL_AMT(args[i]);break;
                    case 23:	obj.setPYMT_STAT_CD(args[i]);break;
                    case 24:	obj.setSTAT_CD(args[i]);break;
                    case 25:	obj.setBILL_EXMPT_FLG(args[i]);break;
                    case 26:	obj.setIDNT_USER_ID(args[i]);break;
                    case 27:	obj.setIDNT_TS(args[i]);break;
                    case 28:	obj.setLAST_UPDT_SRC_ID(args[i]);break;
                    case 29:	obj.setLAST_UPDT_TS(args[i]);break;
                    case 30:	obj.setLAST_UPDT_USER_ID(args[i]);break;
                    case 31:	obj.setTAX_SERV_AS_OF_DT(args[i]);break;
                    case 32:	obj.setCNTY_ID(args[i]);break;
                    case 33:	obj.setSTATE_ID(args[i]);break;
                    case 34:	obj.setPYMT_PASS_THRU(args[i]);break;
                }
            }
        }

        log.info("--PAS Bills--"+obj.toString());

        return obj;
    }
    public PasBillAmt  mapToBillAmt(String[] args)
    {
        PasBillAmt obj = new PasBillAmt();

        for (int i=0;i<args.length;i++) {
            if (args[i] != null) {
                switch (i) {

                    case 0: 	obj.setPRCL_KEY(args[i]);break;
                    case 1: 	obj.setSOR_CD(args[i]);break;
                    case 2: 	obj.setLIEN_KEY(args[i]);break;
                    case 3: 	obj.setBILL_KEY(args[i]);break;
                    case 4: 	obj.setPRCL_BILL_INSTL_KEY(args[i]);break;
                    case 5: 	obj.setPRCL_BILL_AMT_KEY(args[i]);break;
                    case 6: 	obj.setCMNT_KEY(args[i]);break;
                    case 7: 	obj.setBILL_AMT_TYP(args[i]);break;
                    case 8: 	obj.setCRNCY_TYP(args[i]);break;
                    case 9: 	obj.setBILL_AMT(args[i]);break;
                    case 10:	obj.setGOOD_THRU_DT(args[i]);break;
                    case 11:	obj.setORIG_SRC_CD(args[i]);break;
                    case 12:	obj.setMAN_RSRCH_CD(args[i]);break;
                    case 13:	obj.setSTAT_CD(args[i]);break;
                    case 14:	obj.setSTD_FRMT_CNTRL_KEY(args[i]);break;
                    case 15:	obj.setIDNT_USER_ID(args[i]);break;
                    case 16:	obj.setIDNT_TS(args[i]);break;
                    case 17:	obj.setLAST_UPDT_SRC_ID(args[i]);break;
                    case 18:	obj.setLAST_UPDT_TS(args[i]);break;
                    case 19:	obj.setLAST_UPDT_USER_ID(args[i]);break;
                    case 20:	obj.setTAX_SERV_AS_OF_DT(args[i]);break;
                    case 21:	obj.setCNTY_ID(args[i]);break;
                    case 22:	obj.setSTATE_ID(args[i]);break;
                }
            }
        }

        //merge from install code
        //obj.setINSTL_CD(args[0]);
        log.info("--PAS Bill Amounts--"+obj.toString());

        return obj;
    }

}
