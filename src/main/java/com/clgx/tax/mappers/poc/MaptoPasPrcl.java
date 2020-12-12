package com.clgx.tax.mappers.poc;

import com.clgx.tax.data.model.poc.*;

public class MaptoPasPrcl {
    public PasPrcl maptoprcl(String[] arr)
    {
        PasPrcl obj = new PasPrcl();
        obj.setPRCL_KEY(arr[0]);
        obj.setSOR_CD(arr[1]);
        obj.setSTD_FRMT_CNTRL_KEY(arr[2]);
        obj.setPRCL_CMNT_KEY(arr[3]);
        obj.setPRCL_TYP(arr[4]);
        obj.setSTAT_CD(arr[5]);
        obj.setSTRT_PRE_DIR_CD(arr[6]);
        obj.setSTRT_NBR_TXT(arr[7]);
        obj.setSTRT_NBR_END_TXT(arr[8]);
        obj.setSTRT_NBR_FRCTN_TXT(arr[9]);
        obj.setSTRT_NM(arr[10]);
        obj.setSTRT_TYP(arr[11]);
        obj.setSTRT_POST_DIR_CD(arr[12]);
        obj.setUNIT_TYP(arr[13]);
        obj.setUNIT_NBR_TXT(arr[14]);
        obj.setUNIT_NBR_END_TXT(arr[15]);
        obj.setPO_BOX_NBR_TXT(arr[16]);
        obj.setBOX_LOC_TXT(arr[17]);
        obj.setRR_CD(arr[18]);
        obj.setRR_NBR(arr[19]);
        obj.setNGHBRHD_NM(arr[20]);
        obj.setCITY_NM(arr[21]);
        obj.setCNTY_NM(arr[22]);
        obj.setPOSTAL_CD(arr[23]);
        obj.setCNTRY_CD(arr[24]);
        obj.setSTATE_CD(arr[25]);
        obj.setSTATE_SUB_CD(arr[26]);
        obj.setGEO_SRC_CD(arr[27]);
        obj.setFIPS_STATE_ID(arr[28]);
        obj.setFIPS_CNTY_ID(arr[29]);
        obj.setPRCL_USE_CD(arr[30]);
        obj.setSRVY_ID(arr[31]);
        obj.setPRCL_SCND_USE_NM(arr[32]);
        obj.setBLDG_CLS_CD(arr[33]);
        obj.setCNSTCT_YR(arr[34]);
        obj.setLAND_TYP_CD(arr[35]);
        obj.setLAND_USE_NM(arr[36]);
        obj.setPRPTY_SOLD_DT(arr[37]);
        obj.setLAST_IMPRV_YR(arr[38]);
        obj.setOWN_ACQ_DT(arr[39]);
        obj.setESMT_TYP_CD(arr[40]);
        obj.setPRPTY_ZONE_CD(arr[41]);
        obj.setCENSUS_TRACT_CD(arr[42]);
        obj.setFLOOD_ZONE_CD(arr[43]);
        obj.setNO_TXAUTH_FLG(arr[44]);
        obj.setORIG_SRC_CD(arr[45]);
        obj.setMOBL_HOME_VIN_ID(arr[46]);
        obj.setASSESS_PRCL_ID(arr[47]);
        obj.setASSESS_YR(arr[48]);
        obj.setFILE_CRTE_DT(arr[49]);
        obj.setNON_ADV_PRPTY_DESC(arr[50]);
        obj.setNON_ADV_PRPTY_VALUE_AMT(arr[51]);
        obj.setMAN_RSRCH_CD(arr[52]);
        obj.setUAT_ADDR_TAG_ID(arr[53]);
        obj.setPRCL_USE_NM(arr[54]);
        obj.setPRCL_SCND_USE_CD(arr[55]);
        obj.setFULL_EXMPT_FLG(arr[56]);
        obj.setTAX_AREA(arr[57]);
        obj.setLAND_TYP_NM(arr[58]);
        obj.setBLDG_CLS_NM(arr[59]);
        obj.setNGHBRHD_CD(arr[60]);
        obj.setGEO_CD_LATTD(arr[61]);
        obj.setGEO_CD_LONGTD(arr[62]);
        obj.setMAP_GRID_LOC_TXT(arr[63]);
        obj.setUNSCRB_SITUS_ADDR_TXT(arr[64]);
        obj.setUNSCRB_SITUS_ADDR_LN2_TXT(arr[65]);
        obj.setCNTY_ID(arr[66]);
        obj.setSTATE_ID(arr[67]);
        obj.setIDNT_CHNGD_FLG(arr[68]);
        obj.setIDNT_CNFRMD_FLG(arr[69]);
        obj.setIDNT_ID(arr[70]);
        obj.setLAST_UPDT_SRC_ID(arr[71]);
        obj.setLAST_UPDT_TS(arr[72]);
        obj.setLAST_UPDT_USER_ID(arr[73]);
     //   obj.setTAX_SERV_AS_OF_DT(arr[74]);

        return obj;
    }

    public PasLiens mapToLiens(String[] arr)
    {
        PasLiens obj = new PasLiens();
        obj.setPRCL_KEY(arr[0]);
        obj.setSOR_CD(arr[1]);
        obj.setLIEN_KEY(arr[2]);
        obj.setTAX_ID(arr[3]);
        obj.setTXAUTH_ID(arr[4]);
        obj.setCNTRY_CD(arr[5]);
        obj.setSTATE_ID(arr[6]);
        obj.setCNTY_ID(arr[7]);
        obj.setSTAT_CD(arr[8]);
        obj.setSTD_FRMT_CNTRL_KEY(arr[9]);
        obj.setPRCL_CMNT_KEY(arr[10]);
        obj.setTXAUTH_FILE_TYP(arr[11]);
        obj.setLIEN_TYP(arr[12]);
        obj.setDSPLY_TAX_ID_FLG(arr[13]);
        obj.setMUNI_CD(arr[14]);
        obj.setSCHL_DSTRC_CD(arr[15]);
        obj.setPEND_APRTN_FLG(arr[16]);
        obj.setPEND_APRTN_DT(arr[17]);
        obj.setORIG_SRC_CD(arr[18]);
        obj.setFILLER1(arr[19]);
        obj.setLGCY_PRIM_TXAUTH_ID(arr[20]);
        obj.setLGCY_PRIM_TAX_ID(arr[21]);
        obj.setPREV_TAX_ID(arr[22]);
        obj.setPRIOR_YR_DELQ_FLG(arr[23]);
        obj.setMAN_RSRCH_CD(arr[24]);
        obj.setIDNT_USER_ID(arr[25]);
        obj.setIDNT_CHNGD_FLG(arr[26]);
        obj.setIDNT_CNFRMD_FLG(arr[27]);
        obj.setIDNT_METH_CD(arr[28]);
        obj.setIDNT_ID(arr[29]);
        obj.setIDNT_TS(arr[30]);
        obj.setLAST_UPDT_USER_ID(arr[31]);
        obj.setLAST_UPDT_SRC_ID(arr[32]);
        obj.setLAST_UPDT_TS(arr[33]);
       // obj.setTAX_SERV_AS_OF_DT(arr[34]);
     //   obj.setPEND_APRTN_RSN_CD (arr[35]);

        return obj;
    }


    public PasBillsInst mapToBillsInst(String[] args)
    {
        PasBillsInst obj = new PasBillsInst();
        obj.setPRCL_KEY(args[0]);
        obj.setSOR_CD(args[1]);
        obj.setLIEN_KEY(args[2]);
        obj.setBILL_KEY(args[3]);
        obj.setPRCL_BILL_INSTL_KEY(args[4]);
        obj.setPYMT_STAT_CD(args[5]);
        obj.setSTD_FRMT_CNTRL_KEY(args[6]);
        obj.setCMNT_KEY(args[7]);
        obj.setINSTL_CD(args[8]);
        obj.setTAX_BILL_ID(args[9]);
        obj.setIDNT_EXCPT_CD(args[10]);
        obj.setTAX_PYMT_SRC_CD(args[11]);
        obj.setDELQ_DT(args[12]);
        obj.setPD_DT(args[13]);
        obj.setTXAUTH_POST_DT(args[14]);
        obj.setMAIL_DT(args[15]);
        obj.setTXAUTH_TAX_SRCH_DT(args[16]);
        obj.setTXAUTH_FILE_TYP(args[17]);
        obj.setORIG_SRC_CD(args[18]);
        obj.setFILE_CRTE_DT(args[19]);
        obj.setFILE_AQRD_DT(args[20]);
        obj.setMAN_RSRCH_CD(args[21]);
        obj.setDELAY_BILL_CD(args[22]);
        obj.setTXAUTH_ZERO_AMT_CD(args[23]);
        obj.setRDMPTN_SPCL_DOC_CD(args[24]);
        obj.setPAYEE_ID(args[25]);
        obj.setSTAT_CD(args[26]);
        obj.setNSF_FLG(args[27]);
        obj.setIDNT_USER_ID(args[28]);
        obj.setIDNT_TS(args[29]);
        obj.setLAST_UPDT_SRC_ID(args[30]);
        obj.setLAST_UPDT_TS(args[31]);
        obj.setLAST_UPDT_USER_ID(args[32]);
        obj.setTAX_SERV_AS_OF_DT(args[33]);
        obj.setCNTY_ID(args[34]);
        obj.setSTATE_ID(args[35]);
        return obj;
    }

    public PasBills mapToBills(String[] args)
    {
        PasBills obj = new PasBills();
        obj.setPRCL_KEY(args[0]);
        obj.setSOR_CD(args[1]);
        obj.setLIEN_KEY(args[2]);
        obj.setBILL_KEY(args[3]);
        obj.setSTD_FRMT_CNTRL_KEY(args[4]);
        obj.setCMNT_KEY(args[5]);
        obj.setBILL_TYP(args[6]);
        obj.setTAX_BILL_BGN_YR(args[7]);
        obj.setTAX_BILL_END_YR(args[8]);
        obj.setTXAUTH_TAX_FOR_BGN_YR(args[9]);
        obj.setTXAUTH_TAX_FOR_END_YR(args[10]);
        obj.setTXAUTH_BILL_TAX_ID(args[11]);
        obj.setPYMT_PLAN_ID(args[12]);
        obj.setPYMT_PLAN_CD(args[13]);
        obj.setPYMT_PLAN_STAT_CD(args[14]);
        obj.setORIG_SRC_CD(args[15]);
        obj.setBILL_CD(args[16]);
        obj.setBILL_STAT_CD(args[17]);
        obj.setBILL_UID(args[18]);
        obj.setMAN_RSRCH_CD(args[19]);
        obj.setPYMT_PLAN_DFLT_DT(args[20]);
        obj.setPYMT_PLAN_LAST_PAY_DT(args[21]);
        obj.setPYMT_PLAN_UNPD_BAL_AMT(args[22]);
        obj.setPYMT_STAT_CD(args[23]);
        obj.setSTAT_CD(args[24]);
        obj.setBILL_EXMPT_FLG(args[25]);
        obj.setIDNT_USER_ID(args[26]);
        obj.setIDNT_TS(args[27]);
        obj.setLAST_UPDT_SRC_ID(args[28]);
        obj.setLAST_UPDT_TS(args[29]);
        obj.setLAST_UPDT_USER_ID(args[30]);
        obj.setTAX_SERV_AS_OF_DT(args[31]);
        obj.setCNTY_ID(args[32]);
        obj.setSTATE_ID(args[33]);
      //  obj.setPYMT_PASS_THRU(args[34]);
        return obj;
    }
    public PasBillAmt  mapToBillAmt(String[] args)
    {
        PasBillAmt obj = new PasBillAmt();
        obj.setYear(args[0]);
        obj.setPRCL_KEY(args[1]);
        obj.setSOR_CD(args[2]);
        obj.setLIEN_KEY(args[3]);
        obj.setBILL_KEY(args[4]);
        obj.setPRCL_BILL_INSTL_KEY(args[5]);
        obj.setPRCL_BILL_AMT_KEY(args[6]);
        obj.setCMNT_KEY(args[7]);
        obj.setBILL_AMT_TYP(args[8]);
        obj.setCRNCY_TYP(args[9]);
        obj.setBILL_AMT(args[10]);
        obj.setGOOD_THRU_DT(args[11]);
        obj.setORIG_SRC_CD(args[12]);
        obj.setMAN_RSRCH_CD(args[13]);
        obj.setSTAT_CD(args[14]);
        obj.setSTD_FRMT_CNTRL_KEY(args[15]);
        obj.setIDNT_USER_ID(args[16]);
        obj.setIDNT_TS(args[17]);
        obj.setLAST_UPDT_SRC_ID(args[18]);
        obj.setLAST_UPDT_TS(args[19]);
        obj.setLAST_UPDT_USER_ID(args[20]);
        obj.setTAX_SERV_AS_OF_DT(args[21]);
        obj.setCNTY_ID(args[22]);
        obj.setSTATE_ID(args[23]);
        //merge from install code
//  obj.setINSTL_CD(args[0]);
        return obj;
    }

}
