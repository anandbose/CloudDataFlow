package com.clgx.tax.beam.pipelines.samples;

import com.google.common.base.Strings;

import javax.swing.text.DateFormatter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateTest {
    public static void main(String args[]) {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss.SSSSSS a");
        String date = "05/7/2019 3:49:19.000000 AM";
        String[] main = date.split(" ");
        String[] str = main[0].split("/");
        StringBuffer sb= new StringBuffer().append(Strings.padStart(str[0],2,'0'));
        sb.append("/").append(Strings.padStart(str[1],2,'0')).append("/").append(str[2]);
    //fix time
        String[] str1 = main[1].split(":");
        sb.append(" ").append(Strings.padStart(str1[0],2,'0')).append(":")
                .append(Strings.padStart(str1[1],2,'0')).append(":")
                .append(Strings.padStart(str1[2],9,'0'))
        .append(" ")
        .append(main[2]);


        LocalDateTime localDateTime = LocalDateTime.now();
      //  String ldtString = formatter.format(localDateTime);
      //  System.out.println(ldtString);

        LocalDateTime dt = LocalDateTime.parse(sb,formatter);
        System.out.println("Date is ::" + dt.toString());
    }
}
