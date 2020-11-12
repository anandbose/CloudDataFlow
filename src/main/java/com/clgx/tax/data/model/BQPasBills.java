package com.clgx.tax.data.model;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
@DefaultCoder(AvroCoder.class)
@Setter
@Getter
public class BQPasBills {

     private String hashKey;
     private String parcelKey;
     private String sorCD;
     private String lienKey;
     private String billKey;
     private String lastUpdtTS;
    public static BQPasBills convertToObj(TableRow row)
    {
        BQPasBills obj = new BQPasBills();
        obj.setHashKey( (String)row.get("HASHKEY"));
        obj.setParcelKey((String) row.get("PRCL_KEY"));
        obj.setSorCD((String) row.get("SOR_CD"));
        obj.setLienKey((String) row.get("LIEN_KEY"));
        obj.setBillKey((String) row.get("BILL_KEY"));



        obj.setLastUpdtTS(LocalDateTime.parse((String) row.get("LAST_UPDT_TS")).toString());
        return obj;

    }

    private static String convertToDateTime(String input)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss.SSSSSS a");

        try {
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
            return LocalDateTime.now().toString();

        }


    }

    public static TableRow convertToRow(BQPasBills  obj)
    {


        return new TableRow()
                .set("HASHKEY",obj.getHashKey())
                .set("PRCL_KEY",obj.getParcelKey())
                .set("SOR_CD",obj.getSorCD())
                .set("LIEN_KEY",obj.getLienKey())
                .set("BILL_KEY",obj.getBillKey())
                .set("LAST_UPDT_TS",convertToDateTime(obj.getLastUpdtTS()));

    }

    public static TableSchema getPasBillsSchema()
    {
        return
                new TableSchema()
                 .setFields(
                         Arrays.asList(
                                 new TableFieldSchema()
                                     .setName("HASHKEY")
                                     .setType("STRING")
                                     .setMode("NULLABLE"),
                                 new TableFieldSchema()
                                         .setName("PRCL_KEY")
                                         .setType("STRING")
                                         .setMode("NULLABLE"),
                                 new TableFieldSchema()
                                         .setName("SOR_CD")
                                         .setType("STRING")
                                         .setMode("NULLABLE"),
                                 new TableFieldSchema()
                                         .setName("LIEN_KEY")
                                         .setType("STRING")
                                         .setMode("NULLABLE"),
                                 new TableFieldSchema()
                                         .setName("BILL_KEY")
                                         .setType("STRING")
                                         .setMode("NULLABLE"),
                                 new TableFieldSchema()
                                         .setName("LAST_UPDT_TS")
                                         .setType("DATETIME")
                                         .setMode("NULLABLE")
                         )
                 );
    }
}
