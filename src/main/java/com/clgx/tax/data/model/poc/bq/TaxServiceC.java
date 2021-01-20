package com.clgx.tax.data.model.poc.bq;


import com.google.api.services.bigquery.model.TableRow;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.util.Date;

@DefaultCoder(AvroCoder.class)
@Setter
@Getter
public class TaxServiceB {
    private Long customerId;
    private Long contractId;
    private String custLoanNumber;
    private String taxId;
    private String agencyId;
    private String billYear;
    private Date taxDueData;
    private Integer taxInstallment;
    private Double taxAmount;
    private Double taxCorrAmount;
    private Long taxRptdId;
    private String taxRptdCd;
    private String delinquentIndicator;
    private String paidIndicator;
    private Date paymentRptdDate;
    private String custServiceType;


    public static String getQuery()
    {
        String query = "SELECT A.CUST_ID" +
                ", A.Cntrct_ID" +
                ", TRIM(A.Cntrct_Lndr_Loan_Nbr) AS Loan_Number" +
                ", TRIM(B.Tax_Id) AS Parcel_ID" +
                ", LPAD(CAST(B.AGCY_ID AS STRING),9,'0') AS Agency_Id" +
                ", C.Bill_Yr" +
                ", C.Tax_Due_Dt" +
                ", C.Instl" +
                ", C.Tax_Amt" +
                ", C.Corr_Amt" +
                ", c.Tax_Rptd_Id" +
                ", C.Tax_Rptd_Cd" +
                ", C.Delq_Ind" +
                ", C.Paid_Ind" +
                ", C.Pmt_Rptd_Dt" +
                ", CASE WHEN A.Serv_Typ_Id = 3 THEN 'ESCROW' ELSE 'NONESCROW'  END AS Service_Type" +
                "FROM taxdw.v_tdwtaxlien B" +
                "LEFT JOIN  taxdw.v_tdwcntrctmstr A ON A.Cntrct_Id = B.Cntrct_Id" +
                "LEFT JOIN taxdw.v_tdwtaxamtdtl C on C.Cntrct_Nbr = B.Cntrct_Id" +
                "   AND C.Tax_Suff = B.Tax_Lien_Sfx " +
                "   where B.Agcy_Id = 40190000 and C.Bill_Yr in ('2020','2021') limit 10";

        return query;
    }

    public static TaxServiceB fromTableRow (TableRow row)
    {
        TaxServiceB obj = new TaxServiceB();
        obj.setCustomerId((Long)row.get(bqServiceSchema.CUST_ID));
        obj.setContractId((Long)row.get(bqServiceSchema.Cntrct_ID));
        obj.setCustLoanNumber((String) row.get(bqServiceSchema.Loan_Number));
       obj.setTaxId((String)row.get(bqServiceSchema.Tax_Id));
       obj.setAgencyId((String)row.get(bqServiceSchema.Agency_Id));
/*
        private String billYear;
        private Date taxDueData;
        private Integer taxInstallment;
        private Double taxAmount;
        private Double taxCorrAmount;
        private Long taxRptdId;
        private String taxRptdCd;
        private String delinquentIndicator;
        private String paidIndicator;
        private Date paymentRptdDate;
        private String custServiceType;
        */

        return obj;
    }

}
