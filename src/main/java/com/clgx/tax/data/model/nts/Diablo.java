package com.clgx.tax.data.model.nts;

import lombok.*;

import java.io.Serializable;
@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class Diablo implements Serializable {
    private String Client_ID;
    private String Client_Supplied_APN;
    private String Client_Supplied_Property_FIPS_CODE;
    private String Property_Match_Code;
    private String Match_Exception_Code;
    private String FIPS_Code;
    private String County_Name;
    private String APN_Unformatted;
    private String APN_Sequence_Number;
    private String Land_Use_Code;
    private String Property_Indicator;
    private String Tax_Amount;
    private String Tax_Year;
    private String Assessed_Value_Total;
    private String Assessed_Value_Land;
    private String Assessed_Value_Improvement;
    private String Market_Value_Total;
    private String Market_Value_Land;
    private String Market_Value_Improvement;
    private String Assessed_Personal_Property_Total;
    private String Assessed_Fixture_Total;
    private String Assessed_Year;
    private String Owner_Full_Name;
    private String Property_Street_Address;
    private String Property_City;
    private String Property_State;
    private String Property_Zip_Code;
    private String Mailing_Street_Address_PO_Box;
    private String Mailing_City;
    private String Mailing_State;
    private String Mailing_Zip_Code;
    private String PROPERTY_TAX_AREA;
    private String PROPERTY_TAX_DISTRICT_COUNTY;
    private String MUNICIPALITY_TAX_DISTRICT;
    private String SCHOOL_DISTRICT;
    private String FIRE_DISTRICT;
    private String WATER_DISTRICT;
}
