package com.clgx.tax.pas.poc.bq.pipeline;


import com.clgx.tax.pas.poc.bq.model.input.*;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class BQReadWrite implements Serializable {
    private   final String PAS_SCHEMA_FILE_PATH = "schema/pas-nested-schema.json";
    private    String projectId;
    private     String dataSet;
    private    String tableName;
    private   Logger log = LoggerFactory.getLogger(BQReadWrite.class);

    public BQReadWrite()
    {

    }

    public BQReadWrite(String projectId,String dataSet,String tableName)
    {
        this.projectId = projectId;
        this.dataSet = dataSet;
        this.tableName = tableName;
    }




    public   TableReference getTableReference()
    {
        TableReference tableSpec =
                new TableReference()
                        .setProjectId(projectId)
                        .setDatasetId(dataSet)
                        .setTableId(tableName)
                ;

        return tableSpec;
    }


    private  String setHashKey(String hashString)
    {
        String sha256hex = Hashing.sha256()
                .hashString(hashString, StandardCharsets.UTF_8)
                .toString();
        return sha256hex;
    }
    public   String getJsonTableSchema()
    {
        String jsonSchema=null;
        try {
            jsonSchema =
                    Resources.toString(
                            Resources.getResource(PAS_SCHEMA_FILE_PATH), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error(
                    "Unable to read {} file from the resources folder!",PAS_SCHEMA_FILE_PATH, e);
        }
        return jsonSchema;
    }

    /***Create mappers here
     *
     */

    public   TableRow getAddressMapping(PasPrcl prcl)
    {
        TableRow addressRow = new TableRow();
        addressRow.set("STRT_PRE_DIR_CD",prcl.getSTRT_PRE_DIR_CD());
        addressRow.set("STRT_NBR_TXT",prcl.getSTRT_NBR_TXT());
        addressRow.set("STRT_NBR_END_TXT",prcl.getSTRT_NBR_END_TXT());
        addressRow.set("STRT_NM",prcl.getSTRT_NM());
        addressRow.set("PO_BOX_NBR_TXT",prcl.getPO_BOX_NBR_TXT());
        addressRow.set("CITY_NM",prcl.getCITY_NM());
        addressRow.set("POSTAL_CD",prcl.getPOSTAL_CD());
        addressRow.set("CNTRY_CD",prcl.getCNTRY_CD());
        return addressRow;
    }

    /***
     *
     * Get the owner mapping
     * @param owner
     * @return
     */
    public  TableRow getOwmerMapping(PasPrclOwn owner)
    {
        TableRow ownRow = new TableRow();


        ownRow.set("OWN_TYP",owner.getOWN_TYP());

        ownRow.set("CMPNY_NM",owner.getCMPNY_NM());
        ownRow.set("LAST_NM",owner.getLAST_NM());
        ownRow.set("MTRNL_LAST_NM",owner.getMTRNL_LAST_NM());
        ownRow.set("FRST_NM",owner.getFRST_NM());
        ownRow.set("MID_NM",owner.getMID_NM());
        ownRow.set("OWN_PFX_NM",owner.getOWN_PFX_NM());
        ownRow.set("OWN_SFX_NM",owner.getOWN_SFX_NM());
        return ownRow;
    }
    /**
     * Map Liens
     * @param liens
     * @return
     */
     public   TableRow getLienMapping(PasLiens liens, Iterable<PasBills> bills, Iterable<PasBillsInst> instments, Iterable<PasBillAmt> amounts)
     {
         TableRow lienRow = new TableRow();
         lienRow.set("LIEN_KEY",liens.getLIEN_KEY());
         lienRow.set("TAX_ID",liens.getTAX_ID());
         lienRow.set("TXAUTH_ID",liens.getTXAUTH_ID());
         lienRow.set("TXAUTH_FILE_TYP",liens.getTXAUTH_FILE_TYP());
         lienRow.set("LIEN_TYP",liens.getLIEN_TYP());
         lienRow.set("DSPLY_TAX_ID_FLG",liens.getDSPLY_TAX_ID_FLG());
         lienRow.set("MUNI_CD",liens.getMUNI_CD());
         lienRow.set("SCHL_DSTRC_CD",liens.getSCHL_DSTRC_CD());
         lienRow.set("PEND_APRTN_FLG",liens.getPEND_APRTN_FLG());
         lienRow.set("PEND_APRTN_DT",convertToDateTime(liens.getPEND_APRTN_DT()));
         lienRow.set("ORIG_SRC_CD",liens.getORIG_SRC_CD());
         lienRow.set("LGCY_PRIM_TXAUTH_ID",liens.getLGCY_PRIM_TXAUTH_ID());
         lienRow.set("LGCY_PRIM_TAX_ID",liens.getLGCY_PRIM_TAX_ID());
         lienRow.set("PREV_TAX_ID",liens.getPREV_TAX_ID());
         lienRow.set("PRIOR_YR_DELQ_FLG",liens.getPRIOR_YR_DELQ_FLG());
         lienRow.set("MAN_RSRCH_CD",liens.getMAN_RSRCH_CD());
         List<TableRow> billsList = new ArrayList<TableRow>();
         for(PasBills bill : bills)
         {
             billsList.add(getBillMapping(bill,instments,amounts));
         }
         //Iterate for the bills
         //lienRow.set("")
         lienRow.set("BILLS",billsList);
         return lienRow;
     }
    private  TableRow getBillMapping(PasBills bill, Iterable<PasBillsInst> instments, Iterable<PasBillAmt> amounts)
    {
        TableRow billRow = new TableRow();
        billRow.set("BILL_TYP",bill.getBILL_TYP());
        billRow.set("TAX_BILL_BGN_YR",bill.getTAX_BILL_BGN_YR());
        billRow.set("TAX_BILL_END_YR",bill.getTAX_BILL_END_YR());
        billRow.set("TXAUTH_TAX_FOR_BGN_YR",bill.getTXAUTH_TAX_FOR_BGN_YR());
        billRow.set("TXAUTH_TAX_FOR_END_YR",bill.getTXAUTH_TAX_FOR_END_YR());
        billRow.set("TXAUTH_BILL_TAX_ID",bill.getTXAUTH_BILL_TAX_ID());
        billRow.set("PYMT_PLAN_ID",bill.getPYMT_PLAN_ID());
        billRow.set("PYMT_PLAN_CD",bill.getPYMT_PLAN_CD());
        billRow.set("PYMT_PLAN_STAT_CD",bill.getPYMT_PLAN_STAT_CD());
        billRow.set("ORIG_SRC_CD",bill.getORIG_SRC_CD());
        billRow.set("BILL_CD",bill.getBILL_CD());
        billRow.set("BILL_STAT_CD",bill.getBILL_STAT_CD());
        billRow.set("BILL_UID",bill.getBILL_UID());
        billRow.set("MAN_RSRCH_CD",bill.getMAN_RSRCH_CD());
        billRow.set("PYMT_PLAN_DFLT_DT",convertToDateTime(bill.getPYMT_PLAN_DFLT_DT()));
        billRow.set("PYMT_PLAN_LAST_PAY_DT",convertToDateTime(bill.getPYMT_PLAN_LAST_PAY_DT()));
        billRow.set("PYMT_PLAN_UNPD_BAL_AMT",convertToAmt(bill.getPYMT_PLAN_UNPD_BAL_AMT()));
        billRow.set("PYMT_STAT_CD",bill.getPYMT_STAT_CD());
        billRow.set("BILL_EXMPT_FLG",bill.getBILL_EXMPT_FLG());
        List<TableRow> installmentList = new ArrayList<TableRow>();
        for(PasBillsInst installment: instments)
        {
            installmentList.add(getInstallmentlMapping(installment,amounts));
        }
        //Iterate for the bills
        //lienRow.set("")
        billRow.set("INSTALLMENTS",installmentList);
        return billRow;
    }



    private   TableRow getInstallmentlMapping(PasBillsInst installment,Iterable<PasBillAmt> amounts)
    {
        TableRow installmentRow = new TableRow();
        installmentRow.set("INSTL_CD",installment.getINSTL_CD());
        installmentRow.set("TAX_BILL_ID",installment.getTAX_BILL_ID());
        installmentRow.set("TAX_PYMT_SRC_CD",installment.getTAX_PYMT_SRC_CD());
        installmentRow.set("DELQ_DT",convertToDateTime(installment.getDELQ_DT()));
        installmentRow.set("PD_DT",convertToDateTime(installment.getPD_DT()));
        installmentRow.set("TXAUTH_POST_DT",convertToDateTime(installment.getTXAUTH_POST_DT()));
        installmentRow.set("MAIL_DT",convertToDateTime(installment.getMAIL_DT()));
        installmentRow.set("TXAUTH_TAX_SRCH_DT",convertToDateTime(installment.getTXAUTH_TAX_SRCH_DT()));
        installmentRow.set("TXAUTH_FILE_TYP",installment.getTXAUTH_FILE_TYP());
        installmentRow.set("ORIG_SRC_CD",installment.getORIG_SRC_CD());
        installmentRow.set("FILE_CRTE_DT",convertToDateTime(installment.getFILE_CRTE_DT()));
        installmentRow.set("FILE_AQRD_DT",convertToDateTime(installment.getFILE_AQRD_DT()));
        installmentRow.set("MAN_RSRCH_CD",installment.getMAN_RSRCH_CD());
        installmentRow.set("DELAY_BILL_CD",installment.getDELAY_BILL_CD());
        installmentRow.set("TXAUTH_ZERO_AMT_CD", installment.getTXAUTH_ZERO_AMT_CD());
        installmentRow.set("RDMPTN_SPCL_DOC_CD",installment.getRDMPTN_SPCL_DOC_CD());
        installmentRow.set("PAYEE_ID",installment.getPAYEE_ID());
        installmentRow.set("STAT_CD",installment.getSTAT_CD());
        installmentRow.set("NSF_FLG",installment.getNSF_FLG());
        List<TableRow> amountList = new ArrayList<TableRow>();
        for (PasBillAmt amount: amounts)
        {
            amountList.add(getAmountMapping(amount));
        }
        installmentRow.set("AMOUNT",amountList);
        return installmentRow;

    }


    private   TableRow getAmountMapping(PasBillAmt amount)
    {
        TableRow amountRow = new TableRow();
        amountRow.set("BILL_AMT_TYP",amount.getBILL_AMT_TYP());
        amountRow.set("CRNCY_TYP",amount.getCRNCY_TYP());
        amountRow.set("BILL_AMT",convertToAmt(amount.getBILL_AMT()));
        amountRow.set("GOOD_THRU_DT",convertToDateTime(amount.getGOOD_THRU_DT()));

        return amountRow;


    }

    private   String convertToDateTime(String input)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss.SSSSSS a");

        try {
            if(input==null || (input!=null && input.trim().equals("")))
            {
                return null;
               // LocalDateTime.now().format(DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSS"));
            }
            String[] main = input.split(" ");
            String[] str = main[0].split("/");
            StringBuffer sb = new StringBuffer().append(Strings.padStart(str[0], 2, '0'));
            sb.append("/").append(Strings.padStart(str[1], 2, '0')).append("/").append(str[2]);
            //fix time
            String[] str1 = main[1].split(":");
            sb.append(" ").append(Strings.padStart(str1[0], 2, '0')).append(":")
                    .append(Strings.padStart(str1[1], 2, '0')).append(":")
                    .append(Strings.padStart(str1[2], 9, '0'))
                    .append(" ")
                    .append(main[2]);

            return (LocalDateTime.parse(sb.toString(), formatter).toString());
        }
        catch (Exception ex)
        {
            System.out.println("Error in parsing date::"+input);
            ex.printStackTrace();
            return LocalDateTime.now().format(DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSS"));

        }


    }

    private   Double convertToAmt(String input)
    {
        if (input == null || input.trim().equals("")) return null;
        if (input != null) {
            return Double.valueOf(input);
        }
        return null;
    }

}
