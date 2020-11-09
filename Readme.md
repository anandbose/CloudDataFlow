Create service account for storage cloud

Download the service key account cred and add the environment variable
export GOOGLE_APPLICATION_CREDENTIALS="/Users/anbose/Downloads/gcpaccounttest-personal-cloudstorage.json"
mvn compile exec:java   -Dexec.mainClass=com.clgx.tax.beam.pipelines.samples.PasBillsSampleCompare -Dexec.args="--runner=DataflowRunner \
--project=spheric-mesh-294917 \
--stagingLocation=gs://gcdf_demo_test/staging \
--templateLocation=gs://gcdf_demo_test/templates/PasBillsSampleCompare-new \
--region=us-central1-a \
--gcpTempLocation=gs://gcdf_demo_test/temp" -Pdataflow-runner


mvn compile exec:java -Dexec.mainClass=com.clgx.tax.beam.pipelines.samples.PasBillsSampleCompare
