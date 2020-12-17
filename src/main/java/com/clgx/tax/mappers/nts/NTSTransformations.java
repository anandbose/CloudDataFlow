package com.clgx.tax.mappers.nts;

import com.clgx.tax.data.model.nts.BotSchema;
import com.clgx.tax.data.model.nts.Diablo;
import com.clgx.tax.data.model.nts.Mergedoutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

public class NTSTransformations {
    Logger log = LoggerFactory.getLogger(NTSTransformations.class);
    public Diablo mapToDiablo(String[] fields) throws  Exception{

        Diablo obj = new Diablo();
        //print the fields
       for (int i=0;i<fields.length;i++)
        {



            if (fields[i]!=null)
            {
                switch(i)
                {
                    case 0:obj.setClient_ID(fields[0]);break;
                    case 1:obj.setClient_Supplied_APN(fields[1]);break;
                    case 2:obj.setClient_Supplied_Property_FIPS_CODE(fields[2]);break;
                    case 3:obj.setProperty_Match_Code(fields[3]);break;
                    case 4:obj.setMatch_Exception_Code(fields[4]);break;
                    case 5:obj.setFIPS_Code(fields[5]);break;
                    case 6:obj.setCounty_Name(fields[6]);break;
                    case 7:obj.setAPN_Unformatted(fields[7]);break;
                    case 8:obj.setAPN_Sequence_Number(fields[8]);break;
                    case 9:obj.setLand_Use_Code(fields[9]);break;
                    case 10:obj.setProperty_Indicator(fields[10]);break;
                    case 11:obj.setTax_Amount(fields[11]);break;
                    case 12:obj.setTax_Year(fields[12]);break;
                    case 13:obj.setAssessed_Value_Total(fields[13]);break;
                    case 14:obj.setAssessed_Value_Land(fields[14]);break;
                    case 15:obj.setAssessed_Value_Improvement(fields[15]);break;
                    case 16:obj.setMarket_Value_Total(fields[16]);break;
                    case 17:obj.setMarket_Value_Land(fields[17]);break;
                    case 18:obj.setMarket_Value_Improvement(fields[18]);break;
                    case 19:obj.setAssessed_Personal_Property_Total(fields[19]);break;
                    case 20:obj.setAssessed_Fixture_Total(fields[20]);break;
                    case 21:obj.setAssessed_Year(fields[21]);break;
                    case 22:obj.setOwner_Full_Name(fields[22]);break;
                    case 23:obj.setProperty_Street_Address(fields[23]);break;
                    case 24:obj.setProperty_City(fields[24]);break;
                    case 25:obj.setProperty_State(fields[25]);break;
                    case 26:obj.setProperty_Zip_Code(fields[26]);break;
                    case 27:obj.setMailing_Street_Address_PO_Box(fields[27]);break;
                    case 28:obj.setMailing_City(fields[28]);break;
                    case 29:obj.setMailing_State(fields[29]);break;
                    case 30:obj.setMailing_Zip_Code(fields[30]);break;
                    case 31:obj.setPROPERTY_TAX_AREA(fields[31]);break;
                    case 32:obj.setPROPERTY_TAX_DISTRICT_COUNTY(fields[32]);break;
                    case 33:obj.setMUNICIPALITY_TAX_DISTRICT(fields[33]!=null ? fields[33] : "");break;
                    case 34:obj.setSCHOOL_DISTRICT(fields[34]!=null ? fields[34] : "");break;
                    case 35:obj.setFIRE_DISTRICT(fields[35]!=null ? fields[35] : "");break;
                    case 36:obj.setWATER_DISTRICT(fields[36]!=null ? fields[36] : "");break;
                }
            }
        }
       if (obj.getClient_ID().equals("1301"))
        {
           log.info("Client ID::"+obj.getClient_ID()+"::"+obj.toString());
        }
        return obj;
    }

    public BotSchema maptoBot(String[] fields)
    {
        BotSchema obj = new BotSchema();

        for (int i=0;i<fields.length;i++)
        {
            if (fields[i]!=null)
            {
                switch(i)
                {
                    case 0:obj.setOrderNumber(fields[i]);break;
                    case 1:obj.setState(fields[i]);break;
                    case 2:obj.setCounty(fields[i]);break;
                    case 3:obj.setTownship(fields[i]);break;
                    case 4:obj.setParcelID(fields[i]);break;
                    case 5:obj.setURL(fields[i]);break;
                    case 6:obj.setStreetAddress(fields[i]);break;
                    case 7:obj.setCity(fields[i]);break;
                    case 8:obj.setOwnerName1(fields[i]);break;
                    case 9:obj.setOwnerName2(fields[i]);break;
                    case 10:obj.setLandUseCode(fields[i]);break;
                    case 11:obj.setSqFtLand(fields[i]);break;
                    case 12:obj.setTaxAreaDescription(fields[i]);break;
                    case 13:obj.setTaxCode(fields[i]);break;
                    case 14:obj.setTaxAreaCode(fields[i]);break;
                    case 15:obj.setAssessmentYear(fields[i]);break;
                    case 16:obj.setInitialValueStatus(fields[i]);break;
                    case 17:obj.setTotalInitialMarketValue(fields[i]);break;
                    case 18:obj.setTotalInitialLimitedValue(fields[i]);break;
                    case 19:obj.setTotalInitialAssessedValue(fields[i]);break;
                    case 20:obj.setLandInitialAssessedValue(fields[i]);break;
                    case 21:obj.setImprovedInitialAssessedValue(fields[i]);break;
                    case 22:obj.setTotalPreviousMarketValue(fields[i]);break;
                    case 23:obj.setTotalPreviousLimitedValue(fields[i]);break;
                    case 24:obj.setTotalPreviousAssessedValue(fields[i]);break;
                    case 25:obj.setLandPreviousAssessedValue(fields[i]);break;
                    case 26:obj.setImprovementPreviousAssessedValue(fields[i]);break;
                }
            }
        }
        if (obj.getOrderNumber().equals("1205"))
        {
           log.info("Client ID::"+obj.getOrderNumber()+"::"+obj.toString());
        }
        return  obj;
    }
    public Mergedoutput getMerged(Diablo db,BotSchema bot)
    {
        Mergedoutput m = new Mergedoutput();
        m.setBotdata(bot);
        m.setDiablo(db);
        return m;
    }

    public String converttoDelimitedString(Mergedoutput m, String delimiter)
    {
        Diablo objDiablo = m.getDiablo();
        BotSchema objBot = m.getBotdata();

        String str =objDiablo.getClient_ID()+delimiter+
                objDiablo.getClient_Supplied_APN()+delimiter+
                objDiablo.getClient_Supplied_Property_FIPS_CODE()+delimiter+
                objDiablo.getProperty_Match_Code()+delimiter+
                objDiablo.getMatch_Exception_Code()+delimiter+
                objDiablo.getFIPS_Code()+delimiter+
                objDiablo.getCounty_Name()+delimiter+
                objDiablo.getAPN_Unformatted()+delimiter+
                objDiablo.getAPN_Sequence_Number()+delimiter+
                objDiablo.getLand_Use_Code()+delimiter+
                objDiablo.getProperty_Indicator()+delimiter+
                objDiablo.getTax_Amount()+delimiter+
                objDiablo.getTax_Year()+delimiter+
                objDiablo.getAssessed_Value_Total()+delimiter+
                objDiablo.getAssessed_Value_Land()+delimiter+
                objDiablo.getAssessed_Value_Improvement()+delimiter+
                objDiablo.getMarket_Value_Total()+delimiter+
                objDiablo.getMarket_Value_Land()+delimiter+
                objDiablo.getMarket_Value_Improvement()+delimiter+
                objDiablo.getAssessed_Personal_Property_Total()+delimiter+
                objDiablo.getAssessed_Fixture_Total()+delimiter+
                objDiablo.getAssessed_Year()+delimiter+
                objDiablo.getOwner_Full_Name()+delimiter+
                objDiablo.getProperty_Street_Address()+delimiter+
                objDiablo.getProperty_City()+delimiter+
                objDiablo.getProperty_State()+delimiter+
                objDiablo.getProperty_Zip_Code()+delimiter+
                objDiablo.getMailing_Street_Address_PO_Box()+delimiter+
                objDiablo.getMailing_City()+delimiter+
                objDiablo.getMailing_State()+delimiter+
                objDiablo.getMailing_Zip_Code()+delimiter+
                objDiablo.getPROPERTY_TAX_AREA()+delimiter+
                objDiablo.getPROPERTY_TAX_DISTRICT_COUNTY()+delimiter+
                objDiablo.getMUNICIPALITY_TAX_DISTRICT()+delimiter+
                objDiablo.getSCHOOL_DISTRICT()+delimiter+
                objDiablo.getFIRE_DISTRICT()+delimiter+
                objDiablo.getWATER_DISTRICT()+delimiter+
                objBot.getOrderNumber()+delimiter+
                objBot.getState()+delimiter+
                objBot.getCounty()+delimiter+
                objBot.getTownship()+delimiter+
                objBot.getParcelID()+delimiter+
                objBot.getURL()+delimiter+
                objBot.getStreetAddress()+delimiter+
                objBot.getCity()+delimiter+
                objBot.getOwnerName1()+delimiter+
                objBot.getOwnerName2()+delimiter+
                objBot.getLandUseCode()+delimiter+
                objBot.getSqFtLand()+delimiter+
                objBot.getTaxAreaDescription()+delimiter+
                objBot.getTaxCode()+delimiter+
                objBot.getTaxAreaCode()+delimiter+
                objBot.getAssessmentYear()+delimiter+
                objBot.getInitialValueStatus()+delimiter+
                objBot.getTotalInitialMarketValue()+delimiter+
                objBot.getTotalInitialLimitedValue()+delimiter+
                objBot.getTotalInitialAssessedValue()+delimiter+
                objBot.getLandInitialAssessedValue()+delimiter+
                objBot.getImprovedInitialAssessedValue()+delimiter+
                objBot.getTotalPreviousMarketValue()+delimiter+
                objBot.getTotalPreviousLimitedValue()+delimiter+
                objBot.getTotalPreviousAssessedValue()+delimiter+
                objBot.getLandPreviousAssessedValue()+delimiter+
                objBot.getImprovementPreviousAssessedValue()+delimiter;


        return str;

    }

    public String getOutputHeader()
    {
        return

                "Client_ID"+"\t"+
                        " Client_Supplied_APN"+"\t"+
                        " Client_Supplied_Property_FIPS_CODE"+"\t"+
                        " Property_Match_Code"+"\t"+
                        " Match_Exception_Code"+"\t"+
                        " FIPS_Code"+"\t"+
                        " County_Name"+"\t"+
                        " APN_Unformatted"+"\t"+
                        " APN_Sequence_Number"+"\t"+
                        " Land_Use_Code"+"\t"+
                        " Property_Indicator"+"\t"+
                        " Tax_Amount"+"\t"+
                        " Tax_Year"+"\t"+
                        " Assessed_Value_Total"+"\t"+
                        " Assessed_Value_Land"+"\t"+
                        " Assessed_Value_Improvement"+"\t"+
                        " Market_Value_Total"+"\t"+
                        " Market_Value_Land"+"\t"+
                        " Market_Value_Improvement"+"\t"+
                        " Assessed_Personal_Property_Total"+"\t"+
                        " Assessed_Fixture_Total"+"\t"+
                        " Assessed_Year"+"\t"+
                        " Owner_Full_Name"+"\t"+
                        " Property_Street_Address"+"\t"+
                        " Property_City"+"\t"+
                        " Property_State"+"\t"+
                        " Property_Zip_Code"+"\t"+
                        " Mailing_Street_Address_PO_Box"+"\t"+
                        " Mailing_City"+"\t"+
                        " Mailing_State"+"\t"+
                        " Mailing_Zip_Code"+"\t"+
                        " PROPERTY_TAX_AREA"+"\t"+
                        " PROPERTY_TAX_DISTRICT_COUNTY"+"\t"+
                        " MUNICIPALITY_TAX_DISTRICT"+"\t"+
                        " SCHOOL_DISTRICT"+"\t"+
                        " FIRE_DISTRICT"+"\t"+
                        " WATER_DISTRICT"+"\t"+
                        "OrderNumber"+"\t"+
                        "State"+"\t"+
                        "County"+"\t"+
                        "Township"+"\t"+
                        "ParcelID"+"\t"+
                        "URL"+"\t"+
                        "StreetAddress"+"\t"+
                        "City"+"\t"+
                        "OwnerName1"+"\t"+
                        "OwnerName2"+"\t"+
                        "LandUseCode"+"\t"+
                        "SqFtLand"+"\t"+
                        "TaxAreaDescription"+"\t"+
                        "TaxCode"+"\t"+
                        "TaxAreaCode"+"\t"+
                        "AssessmentYear"+"\t"+
                        "InitialValueStatus"+"\t"+
                        "TotalInitialMarketValue"+"\t"+
                        "TotalInitialLimitedValue"+"\t"+
                        "TotalInitialAssessedValue"+"\t"+
                        "LandInitialAssessedValue"+"\t"+
                        "ImprovedInitialAssessedValue"+"\t"+
                        "TotalPreviousMarketValue"+"\t"+
                        "TotalPreviousLimitedValue"+"\t"+
                        "TotalPreviousAssessedValue"+"\t"+
                        "LandPreviousAssessedValue"+"\t"+
                        "ImprovementPreviousAssessedValue";
    }
}
