Set google credentials

export GOOGLE_APPLICATION_CREDENTIALS="/Users/anbose/Downloads/dtetl-dev.json"

Compile the file

 ./gradlew clean execute -DmainClass=com.clgx.tax.poc.clip.POCGenerateClip

Create template and export to Google cloud data flow
./gradlew clean execute -DmainClass=com.clgx.tax.poc.clip.POCGenerateClip \
-Dexec.args="--runner=DataflowRunner  \
--project=clgx-dtetl-spark-dev-fc0e  \
--stagingLocation=gs://gcdf_dev_test/staging  \
--templateLocation=gs://gcdf_dev_test/templates/generate-clip-gradle-1  \
--region=us-central1-a  \
--gcpTempLocation=gs://gcdf_dev_test/temp --tempLocation=gs://clgx_gcdf_demo_test/temp"


Execute the dataflow job

gcloud dataflow jobs run run-clipparcel-gradle-04  \
--gcs-location=gs://gcdf_dev_test/templates/generate-clip-gradle-2   \
--region=us-west1     \
--service-account-email=dataflow-service-account@clgx-dtetl-spark-dev-fc0e.iam.gserviceaccount.com     \
--subnetwork=https://www.googleapis.com/compute/v1/projects/clgx-network-nonprd-4dd3/regions/us-west1/subnetworks/clgx-app-us-w1-app-dev-subnet     \
--network=projects/clgx-network-nonprd-4dd3/global/networks/clgx-vpc-nonprd     \
--staging-location=gs://gcdf_dev_test/temp \
--num-workers=2 --max-workers=4       \
--worker-machine-type=n2-standard-4      \
--parameters="filePrefix=gs://gcdf_dev_test/input/-04019-20201218,outputFileName=gs://gcdf_dev_test/output/out-20210121-04019" 


using maven

mvn compile exec:java  -Dexec.mainClass=com.clgx.tax.poc.clip.POCGenerateClip -Dexec.args="--runner=DataflowRunner  \
--project=clgx-dtetl-spark-dev-fc0e  \
--stagingLocation=gs://gcdf_dev_test/staging  \
--templateLocation=gs://gcdf_dev_test/templates/generate-clip-gradle-2  \
--region=us-central1-a  \
--gcpTempLocation=gs://gcdf_dev_test/temp --tempLocation=gs://clgx_gcdf_demo_test/temp"


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
--httpUrl=https://uat-west-clp-coreapi-clip-lookup.apps.uat.pcfusw1stg.solutions.corelogic.com/search/apn"


local runer

mvn compile exec:java  -Dexec.mainClass=com.clgx.tax.poc.clip.POCGenerateClipFlexTemplate \
-Dexec.args=" \
--httpUrl=https://uat-west-clp-coreapi-clip-lookup.apps.uat.pcfusw1stg.solutions.corelogic.com/search/apn"



export PROJECT="clgx-dtetl-spark-dev-fc0e"
export TEMPLATE_IMAGE="gcr.io/$PROJECT/dataflow/clip-template:latest"
gcloud builds submit --tag $TEMPLATE_IMAGE .


