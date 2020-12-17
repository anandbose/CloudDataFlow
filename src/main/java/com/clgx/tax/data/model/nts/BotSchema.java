package com.clgx.tax.data.model.nts;

import lombok.*;

import java.io.Serializable;

@Data
@Getter
@Setter
@NoArgsConstructor
@ToString
public class BotSchema implements Serializable {
    private String OrderNumber;
    private String State;
    private String County;
    private String Township;
    private String ParcelID;
    private String URL;
    private String StreetAddress;
    private String City;
    private String OwnerName1;
    private String OwnerName2;
    private String LandUseCode;
    private String SqFtLand;
    private String TaxAreaDescription;
    private String TaxCode;
    private String TaxAreaCode;
    private String AssessmentYear;
    private String InitialValueStatus;
    private String TotalInitialMarketValue;
    private String TotalInitialLimitedValue;
    private String TotalInitialAssessedValue;
    private String LandInitialAssessedValue;
    private String ImprovedInitialAssessedValue;
    private String TotalPreviousMarketValue;
    private String TotalPreviousLimitedValue;
    private String TotalPreviousAssessedValue;
    private String LandPreviousAssessedValue;
    private String ImprovementPreviousAssessedValue;
}
