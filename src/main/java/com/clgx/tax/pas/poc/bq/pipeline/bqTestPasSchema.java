package com.clgx.tax.pas.poc.bq.pipeline;


import com.clgx.tax.pas.poc.bq.model.input.*;
import com.clgx.tax.pas.poc.bq.model.output.*;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.hash.Hashing;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
@Getter
@Setter
@ToString
public class bqTestPasSchema implements Serializable {



    private CombinedRecordsets recordSets;
    private static final String PAS_SCHEMA_FILE_PATH = "schema/pas-bq-schema.json";
    private static Logger log = LoggerFactory.getLogger(bqTestPasSchema.class);

    public static bqSchema convertToObj(TableRow row)
    {
        bqSchema obj = new bqSchema();
        obj.setParcelKey((String)row.get("PARCELKEY"));
        obj.setHashKey((String)row.get("HASHKEYVAL"));
     //   obj.setAmountKey((String)row.get("AMOUNTKEY"));
        return obj;
    }
    private static String setHashKey(String hashString)
    {
        String sha256hex = Hashing.sha256()
                .hashString(hashString, StandardCharsets.UTF_8)
                .toString();
        return sha256hex;
    }
    public static String getJsonTableSchema()
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

    public static List<KV<String, CombinedRecordsets>> convertToBqSchema(Iterable<PasPrcl> PasPrclrecs,
                                                                         Iterable<PasPrclOwn> PasPrcslOwnrecs,
                                                                         Iterable<PasLiens> PasLienrecs,
                                                                         Iterable<PasBills> PasBillrecs,
                                                                         Iterable<PasBillsInst> PasBillInstrecs,
                                                                         Iterable<PasBillAmt> PasAmtrecs)
    {
        List<KV<String,CombinedRecordsets>> ComboRecords = new ArrayList<>();
        //Create the object
        bqTestPasSchema bqRec;

        for (PasPrcl parcel: PasPrclrecs)
        {
            /**Set Parcel information**/
            Parcel opParcel = new Parcel();
            opParcel.setPrclKey(parcel.getPRCL_KEY());
            opParcel.setStateCounty(parcel.getSTATE_ID()+parcel.getCNTY_ID());
            opParcel.setApnNumber(parcel.getASSESS_PRCL_ID());
            opParcel.setClipNumber(parcel.getClipNumber());
            Address opAddress = new Address();
            opAddress.setUnitNumber(parcel.getUNIT_NBR_TXT()!=null ? parcel.getUNIT_NBR_TXT() : "");
            opAddress.setStreetDirection(parcel.getSTRT_PRE_DIR_CD()!=null ? parcel.getSTRT_PRE_DIR_CD() : "");
            opAddress.setStreetAddress(parcel.getSTRT_NBR_TXT()+" "+parcel.getSTRT_NM()+" "+parcel.getSTRT_TYP());
            opAddress.setCity(parcel.getCITY_NM());
            opAddress.setCountyName(parcel.getCNTY_NM());
            opAddress.setPostalCode(parcel.getPOSTAL_CD());
            opParcel.setAddress(opAddress);
            /**Add the owners**/
            List<Owner> ownerList = new ArrayList<Owner>();
            for (PasPrclOwn powner: getOwners(parcel.getPRCL_KEY(),PasPrcslOwnrecs))
            {

                if(
                        powner.getSOR_CD().equals("TXA")
                )
                {
                    Owner opOwners = new Owner();
                    opOwners.setFirstName(powner.getFRST_NM());
                    opOwners.setLastName(powner.getLAST_NM());
                    opOwners.setOwnerType(powner.getOWN_TYP());
                    opOwners.setOwnerKey(powner.getOWN_KEY());
                    ownerList.add(opOwners);


                }
            }
            opParcel.setOwners(ownerList);
            //get the liens
            List<PasLiens> Liens = getLiens(parcel.getPRCL_KEY(),PasLienrecs);
            List<String> taxIds = new ArrayList<>();
            List<Installment> installments = new ArrayList<>();

            for(PasLiens lien: Liens)
            {
                taxIds.add(lien.getTAX_ID());
                log.info("Lien is ::"+lien.getLIEN_KEY());
                for(PasBills bill : getBills(lien.getLIEN_KEY(),PasBillrecs))
                {
                    log.info("Bill is::"+bill.getBILL_KEY());
                    for(PasBillsInst installment: getInstallments(lien.getLIEN_KEY(),bill.getBILL_KEY(),PasBillInstrecs))
                    {
                        log.info("installment is::"+installment.getPRCL_BILL_INSTL_KEY());
                        Installment opInstallment = new Installment();
                      //  opInstallment.s
                        opInstallment.setInstallmentID(installment.getINSTL_CD());
                        opInstallment.setInstallmentType(installment.getPYMT_STAT_CD());
                        opInstallment.setInstallmentUniqueKey(
                                installment.getPRCL_KEY() + "-" +installment.getLIEN_KEY() +"-"
                                        + installment.getBILL_KEY() + "-"+ installment.getPRCL_BILL_INSTL_KEY()

                        );
                        //Create amount records
                        List<Amount> amounts = new ArrayList<>();
                        List<bqSchema> bqRecords = new ArrayList<>();
                        for (PasBillAmt amount: getAmountRecs(installment.getLIEN_KEY(),
                                installment.getBILL_KEY(),
                                installment.getPRCL_BILL_INSTL_KEY(),
                                PasAmtrecs))
                        {
                            //Set bigquery record



                            //  assert Lien != null;
                            bqSchema obj = new bqSchema();
                            obj.setParcelKey(amount.getPRCL_KEY());
                            obj.setLienKey(amount.getLIEN_KEY());
                            obj.setBillKey(amount.getBILL_KEY());
                            obj.setInstallmentKey(amount.getPRCL_BILL_INSTL_KEY());
                            obj.setAmountKey(amount.getPRCL_BILL_AMT_KEY());
                            obj.setBillYear((bill!=null && bill.getTAX_BILL_BGN_YR()!=null) ? bill.getTAX_BILL_BGN_YR():"");
                            obj.setClipNumber( parcel.getClipNumber()!=null ?  parcel.getClipNumber() : "");
                            obj.setTaxId(lien!=null && lien.getTAX_ID()!=null ? lien.getTAX_ID():"");
                            obj.setApnNumber(parcel.getASSESS_PRCL_ID()!=null ? parcel.getASSESS_PRCL_ID():"");
                            obj.setInstallmentId(installment.getINSTL_CD() !=null ? installment.getINSTL_CD() :"");
                            obj.setAmountType( amount.getBILL_AMT_TYP() !=null ?  amount.getBILL_AMT_TYP() :"");
                            obj.setAmount(amount.getBILL_AMT()!=null?Double.valueOf(amount.getBILL_AMT()):0);
                            String hstr = obj.toString();
                            obj.setHashKey(setHashKey(hstr));

                            bqRecords.add(obj);


                            //create the amount record

                            Amount opAmount = new Amount();
                            try {
                                opAmount.setAmount(Double.valueOf((amount.getBILL_AMT() != null || amount.getBILL_AMT() != "") ? amount.getBILL_AMT() : "0.0"));
                            }
                            catch (Exception ex)
                            {
                                ex.printStackTrace();
                                opAmount.setAmount(Double.valueOf("0.0"));
                            }
                            opAmount.setAmountType(amount.getBILL_AMT_TYP());
                            amounts.add(opAmount);
                            if (opAmount.getAmountType().equals("DUE"))
                            {
                                opInstallment.setAmountTotals(opAmount.getAmount());
                            }

                        }
                        opInstallment.setAmounts(amounts);
                        opInstallment.setBigQueryRecs(bqRecords);
                        installments.add(opInstallment);

                    }
                }



            }
            opParcel.setTaxIds(taxIds);
            opParcel.setInstallments(installments);
            CombinedRecordsets recset = new CombinedRecordsets();
            recset.setPrcl(opParcel);
            KV<String,CombinedRecordsets>comboRecord = KV.of(opParcel.getPrclKey(),recset);
            ComboRecords.add(comboRecord);
        }


        return ComboRecords;
    }

    public  static List<TableRow> convertToRow(CombinedRecordsets combRecs, Long Days)
    {
        TableRow row;
        List<TableRow> rows = new ArrayList<TableRow>();
        Parcel prcl=null;


            prcl = combRecs.getPrcl();
        for(Installment installment: prcl.getInstallments())
        {
            for (bqSchema bqRec: installment.getBigQueryRecs())
            {
                log.info("The hash String is "+bqRec.getHashKey());
                Owner owner = getOwner(prcl.getOwners());
                row = new TableRow()
                        .set("HASHKEYVAL",bqRec.getHashKey())
                        .set("PARCELKEY",bqRec.getParcelKey())
                        .set("LIENKEY",bqRec.getLienKey())
                        .set("BILLKEY",bqRec.getBillKey())
                        .set("INSTALLMENTKEY",bqRec.getInstallmentKey())
                        .set("AMOUNTKEY",bqRec.getAmountKey())

                        .set("BILLYEAR",bqRec.getBillYear()!=null ? bqRec.getBillYear() : "")
                        .set("CLIPNUMBER",bqRec.getClipNumber()!=null ? bqRec.getClipNumber():"")
                        .set("TAXID",(bqRec.getTaxId() !=null )? bqRec.getTaxId() : "")
                        .set("STATECOUNTY",prcl.getStateCounty()!=null ? prcl.getStateCounty() :"")
                        .set("APNNUMBER",prcl.getApnNumber())
                        .set("ADD_UNITNUMBER",prcl.getAddress().getUnitNumber()!=null ? prcl.getAddress().getUnitNumber(): "")
                        .set("ADD_STREETDIRECTION",prcl.getAddress().getStreetDirection()!=null ? prcl.getAddress().getStreetDirection() : "")
                        .set("ADD_STREETADDRESS",prcl.getAddress().getStreetAddress()!=null? prcl.getAddress().getStreetAddress():"")
                        .set("ADD_CITY",prcl.getAddress().getCity())
                        .set("ADD_POSTALCODE",prcl.getAddress().getPostalCode())
                        .set("ADD_COUNTYNAME",prcl.getAddress().getCountyName())
                        .set("OWNER_PRIMARY_FIRSTNAME",(owner!=null && owner.getFirstName()!=null) ? owner.getFirstName():"")
                        .set("OWNER_PRIMARY_LASTNAME",(owner!=null && owner.getLastName()!=null) ? owner.getLastName():"")

                        .set("INSTALLMENTID",installment.getInstallmentID())
                        .set("AMOUNTTYPE" , bqRec.getAmountType())
                        .set("AMOUNT" , bqRec.getAmount()!=null? Double.valueOf(bqRec.getAmount()):0)
                        .set("GOOD_THRU_DT" , LocalDateTime.now().minusDays(Days).format(DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSS")));

                rows.add(row);
            }
        }
        return rows;
    }

     private static PasBills getBill(PasBillAmt amount, Iterable<PasBills> Bills)
     {
         for (PasBills bill : Bills)
         {
             if (bill.getBILL_KEY().equals(amount.getBILL_KEY())
                     && bill.getLIEN_KEY().equals(amount.getLIEN_KEY())

             )
                 return bill;
         }
         return null;
     }
    private static List<PasBills> getBills(String lienKey, Iterable<PasBills> Bills)
    {
        List<PasBills> billList = new ArrayList<>();
        for (PasBills bill : Bills)
        {
            if (
                     bill.getLIEN_KEY().equals(lienKey)

            )
                billList.add(bill);
        }
        return billList;
    }
    private static PasLiens getLien(PasBillAmt amount,Iterable<PasLiens> Liens)
    {
        for (PasLiens Lien : Liens)
        {
            if (
                     Lien.getLIEN_KEY().equals(amount.getLIEN_KEY())

            )
                return Lien;
        }
        return null;
    }
    private static List<PasLiens> getLiens(String parcelKey, Iterable<PasLiens> Liens)
    {
        List<PasLiens > liensList = new ArrayList<>();
        for (PasLiens Lien : Liens)
        {
            if (
                    Lien.getPRCL_KEY().equals(parcelKey)

            )
                liensList.add(Lien);
        }
        return liensList;
    }
    private static Owner getOwner(List<Owner> owners)
    {
        for (Owner owner : owners)
        {
            if (
                     owner.getOwnerType()!=null
                     && owner.getOwnerType().equals("OWN")

            )
                return owner;
        }
        return null;
    }

    private static List<PasPrclOwn> getOwners(String key, Iterable<PasPrclOwn> Owners)
    {
        List<PasPrclOwn> ownersList = new ArrayList<>();
        for (PasPrclOwn owner : Owners)
        {
            if (
                    owner.getPRCL_KEY().equals(key)


            )
            {
                ownersList.add(owner);
            }

        }
        return ownersList;
    }
    private static PasBillsInst getInstallment(PasBillAmt amount, Iterable<PasBillsInst> installments)
    {
        for(PasBillsInst installment: installments)
        {
            if(
                    amount.getLIEN_KEY().equals(installment.getLIEN_KEY())
                    && amount.getBILL_KEY().equals(installment.getBILL_KEY())
                    && amount.getPRCL_BILL_INSTL_KEY().equals(installment.getPRCL_BILL_INSTL_KEY())
            )
                return installment;
        }
        return null;
    }

    private static List<PasBillsInst> getInstallments(String lienKey, String billKey, Iterable<PasBillsInst> installments)
    {
        List<PasBillsInst> installmentList = new ArrayList<>();
        for(PasBillsInst installment: installments)
        {
            if(
                    lienKey.equals(installment.getLIEN_KEY())
                            && billKey.equals(installment.getBILL_KEY())
                           )

                installmentList.add(installment) ;
        }
        return installmentList;
    }

    private static List<PasBillAmt> getAmountRecs(String lienKey, String billKey, String instkey, Iterable<PasBillAmt> amounts)
    {
        List<PasBillAmt> amountList = new ArrayList<>();
        for(PasBillAmt amount : amounts)
        {
            if(
                    amount.getLIEN_KEY().equals(lienKey)
                    && amount.getBILL_KEY().equals(billKey)
                    && amount.getPRCL_BILL_INSTL_KEY().equals(instkey)
            )
                amountList.add(amount);
        }
        return amountList;
    }
    public static TableSchema getPasSchema() {

        TableSchema schema =
                new TableSchema()
                        .setFields(
                                Arrays.asList(
                                        new TableFieldSchema()
                                                .setName("PARCELKEY")
                                                .setType("STRING")
                                                .setMode("REQUIRED"),
                                        new TableFieldSchema()
                                                .setName("LIENKEY")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("BILLKEY")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("INSTALLMENTKEY")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("AMOUNTKEY")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("BILLYEAR")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("CLIPNUMBER")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("TAXID")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("STATECOUNTY")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("APNNUMBER")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("ADD_UNITNUMBER")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("ADD_STREETDIRECTION")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("ADD_STREETADDRESS")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("ADD_CITY")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("ADD_POSTALCODE")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("ADD_COUNTYNAME")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("OWNER_PRIMARY_FIRSTNAME")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),

                                        new TableFieldSchema()
                                                .setName("OWNER_PRIMARY_LASTNAME")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("INSTALLMENTID")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("INSTALLMENTTYPE")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("AMOUNTTYPE")
                                                .setType("STRING")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("AMOUNT")
                                                .setType("FLOAT64")
                                                .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                                .setName("GOOD_THRU_DT")
                                                .setType("DATETIME")
                                                .setMode("NULLABLE")
                                )
                        );
        return schema;

    }

}
