# Clip Service - Flex template

This is a POC for invoking clip service as part of a dataflow jon

- Read the PAS Parcel data
- Create the requests into batches
- Call the http service synchronously
- Create the CLIP output file

Future Scope::
- Add Big query table, make sure only non-clipped records are requested



# Setup Steps

- [ ] Setup and authenticate Google cloud account

- [ ] Setup bucket for the application

- [ ] Set the cloud app credentials variable
```bash      
 export GOOGLE_APPLICATION_CREDENTIALS="/Users/anbose/Downloads/dtetl-dev.json"
 ```

# Execution Steps

- [ ] Compile and run the code locally

```bash
mvn compile exec:java  -Dexec.mainClass=com.clgx.tax.poc.clip.POCGenerateClipFlexTemplate \
-Dexec.args=" \
--httpUrl=https://uat-west-clp-coreapi-clip-lookup.apps.uat.pcfusw1stg.solutions.corelogic.com/search/apn \
--apiKey=xAbaGhS2orRCICWSAYiKXfBUHBrY1S90"
```
- [ ] Compile the code and export the dataflow template to cloud
```bash
mvn compile exec:java  -Dexec.mainClass=com.clgx.tax.poc.clip.POCGenerateClipFlexTemplate \
-Dexec.args=" \
--project=clgx-dtetl-spark-dev-fc0e \
--runner=DataflowRunner \
--region=us-west1     \
--gcpTempLocation=gs://gcdf_dev_test/temp  \
--serviceAccount=dataflow-service-account@clgx-dtetl-spark-dev-fc0e.iam.gserviceaccount.com     \
--subnetwork=https://www.googleapis.com/compute/v1/projects/clgx-network-nonprd-4dd3/regions/us-west1/subnetworks/clgx-app-us-w1-app-dev-subnet     \
--network=projects/clgx-network-nonprd-4dd3/global/networks/clgx-vpc-nonprd     \
--numWorkers=1 \
--maxNumWorkers=10 \
--filePrefix=gs://gcdf_dev_test/input/-04019-20201218 \
--outputFileName=gs://gcdf_dev_test/output/out-20210121-04019  \
--httpUrl=https://uat-west-clp-coreapi-clip-lookup.apps.uat.pcfusw1stg.solutions.corelogic.com/search/apn \
--apiKey=xAbaGhS2orRCICWSAYiKXfBUHBrY1S90"
```




