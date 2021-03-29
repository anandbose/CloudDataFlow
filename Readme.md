
- Run Local
```androiddatabinding
export GOOGLE_APPLICATION_CREDENTIALS="/Users/anbose/Downloads/dtetl-dev.json"


```
```bash
 mvn compile exec:java -Dexec.mainClass=com.clgx.tax.pas.poc.bq.POCPasDataProcessFlex \
   -Dexec.args="--tempLocation=gs://gcdf_dev_test/temp  \
   --days=6 \
   --project=clgx-dtetl-spark-dev-fc0e \
   --partitionDate=2021-01-28"
 
   mvn compile exec:java -Dexec.mainClass=com.clgx.tax.pas.poc.bq.POCPasDataProcessFlex -Dexec.args="--tempLocation=gs://gcdf_dev_test/temp --days=5 --project=clgx-dtetl-spark-dev-fc0e --partitionDate=2021-01-31"
  
   
```

- Run on GCP

```bash
 mvn compile exec:java -Dexec.mainClass=com.clgx.tax.pas.poc.bq.POCPasDataProcessFlex -Dexec.args="\
--project=clgx-dtetl-spark-dev-fc0e \
--runner=DataflowRunner \
--region=us-west1     \
--serviceAccount=dataflow-service-account@clgx-dtetl-spark-dev-fc0e.iam.gserviceaccount.com     \
--subnetwork=https://www.googleapis.com/compute/v1/projects/clgx-network-nonprd-4dd3/regions/us-west1/subnetworks/clgx-app-us-w1-app-dev-subnet     \
--network=projects/clgx-network-nonprd-4dd3/global/networks/clgx-vpc-nonprd     \
--numWorkers=1 \
--maxNumWorkers=50 \
--workerMachineType=n2-standard-4 \
--filePrefix=gs://gcdf_dev_test/input/-04019-20201218 \
--outputFileName=gs://gcdf_dev_test/output/out-04019-20201218 \
--elasticUrl=https://cc317dd9125743c9a2f563cf4d48dd06.int-ece-main-green-proxy.elastic.int.idap.clgxdata.com:9243 \
--elasticUsername=clgx_service \
--elasticPwd=clgx_service_r0ck$ \
--days=6 \
--partitionDate=2021-01-20"


```

Create the table

bq mk --table  \
--time_partitioning_field GOOD_THRU_DT  \
--time_partitioning_type DAY  \
exploratory.pas_data_temp ./src/main/resources/schema/pas-bq-schema-commandline.json

mvn compile exec:java -Dexec.mainClass=com.clgx.tax.beam.pipelines.samples.POCPasDataProcessFlex -Dexec.args="--tempLocation=gs://gcdf_dev_test/temp --days=2 --project=clgx-dtetl-spark-dev-fc0e --partitionDate=2021-01-30"

bq query --nouse_legacy_sql "delete from exploratory.pas_data_temp where HASHKEYVAL in (select HASHKEYVAL from exploratory.pas_data_temp where GOOD_THRU_DT >=  '2021-01-27' )"

bq query --nouse_legacy_sql "delete from exploratory.pas_data_temp where HASHKEYVAL in \
('78a1a94a5666d20483922a481e24f442a364163471773c7a6b11b8094d400d01', \
'990041b14ce6399d9221fcd67c38b61b6f72f760c784b53bf6d1f685f5ef15ca' , \
'44cf0474f873a390856b9483384286d2ae63f32b688f3fe018425df29aaf562e')"


bq mk --table exploratory.pas_nested_table_04019_R1 ./src/main/resources/schema/pas-nested-schema-commandline.json

bq mk --table exploratory.pas_nested_table_04019_R2 ./src/main/resources/schema/pas-nested-schema-commandline.json

bq mk --table exploratory.pas_nested_table_04019_revision ./src/main/resources/schema/revision-schema-commandline
# Pas Schema for bq
bq rm --table 

#spindle table
bq rm --table exploratory.pas_new_schema_test
bq mk --table exploratory.pas_new_schema_test ./src/main/resources/schema/pas-bq-new-schema-commandline.json
 

#query idap table
bq query --nouse_legacy_sql 'select count(*) from `clgx-idap-bigquery-dev-71f0.edr_pmd_property_pipeline.vw_ext_clip_address_xref`'

bq query --nouse_legacy_sql 'select clip,apnUnformatted,eapFipsCountyCode from `clgx-idap-bigquery-dev-71f0.edr_pmd_property_pipeline.vw_ext_clip_address_xref` LIMIT 10'

bq query --nouse_legacy_sql 'select clip,apnUnformatted,eapFipsCountyCode from `clgx-idap-bigquery-dev-71f0.edr_pmd_property_pipeline.vw_ext_clip_address_xref`  where eapFipsCountyCode="06037" LIMIT 10'

bq query --nouse_legacy_sql 'select distinct(eapFipsCountyCode) as FipsCode from`clgx-idap-bigquery-dev-71f0.edr_pmd_property_pipeline.vw_ext_clip_address_xref` where eapFipsCountyCode like "04%"  '


