title: geobeam: a distributed computing framework for spatial data
authors: Zhenwne He, Gang Liu, Xiaogang Ma, Qiyu Chen

The geobeam is an open source distributed computing framework for spatial data.
The test datasets are from SpatialHadoop(http://spatialhadoop.cs.umn.edu/datasets.html)

root@namenode1:~# cd /gtl
[only run once] root@namenode1:/gtl# svn co https://112.74.79.128/svn/gtl/trunk/gtl
root@namenode1:/gtl# cd gtl
root@namenode1:svn update
root@namenode1:mkdir ~/gtl
root@namenode1:mkdir ~/gtl/data
root@namenode1:cp -rf /gtl/gtl/data/dat ~/gtl/data/dat
root@namenode1:mvn install
root@namenode1:cd /gtl/geobeam
[only run once] root@namenode1:/gtl# svn co https://112.74.79.128/svn/gtl/trunk/geobeam
root@namenode1:svn update
root@namenode1:mvn install
root@namenode1:java -cp ./target/geobeam-bundled-1.0-SNAPSHOT.jar  gtl.beam.app.BeamSDBApp
root@namenode1:mvn package exec:java -Dexec.mainClass=gtl.beam.app.BeamSDBApp  -Dexec.args="--inputFile=/home/vincent/gtl/data/dat/counties/county_small.tsv  --outputFile=/home/vincent/gtl/data/dat/temp/county_small_temp --runner=FlinkRunner" -Pflink-runner

[run spark cluster, yarn]
root@namenode1:mvn package
root@namenode1:spark-submit --class gtl.beam.app.BeamSDBApp --master yarn --deploy-mode cluster --driver-memory 4g  --executor-memory 1g --executor-cores 1 --queue thequeue ./target/geobeam-bundled-1.0-SNAPSHOT.jar
root@namenode1:spark-submit --class gtl.beam.app.BeamSDBApp --master yarn --deploy-mode cluster --driver-memory 4g  --executor-memory 1g   ./target/geobeam-bundled-1.0-SNAPSHOT.jar
root@namenode1:mvn compile exec:java -Dexec.mainClass=gtl.beam.app.BeamSDBApp -Dexec.args="--runner=SparkRunner" -Pspark-runner
root@namenode1:mvn package exec:java -Dexec.mainClass=gtl.beam.app.BeamSDBApp  -Dexec.args="--inputFile=/home/vincent/gtl/data/dat/counties/county_small.tsv  --outputFile=/home/vincent/gtl/data/dat/temp/county_small_temp --runner=FlinkRunner" -Pflink-runner
































$> mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=2.7.0 \
      -DgroupId=gtl \
      -DartifactId=geobeam \
      -Dversion="0.1" \
      -Dpackage=gtl.geobeam \
      -DinteractiveMode=false

$> mvn package exec:java -Dexec.mainClass=gtl.geobeam.WordCount -Dexec.args="--inputFile=pom.xml --output=counts" -Pdirect-runner

$> java -cp ./target/geobeam-bundled-1.0.jar  gtl.beam.app.BeamSDBApp


$> mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount -Dexec.args="--inputFile=pom.xml --output=counts --runner=ApexRunner" -Papex-runner


$> mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount  -Dexec.args="--runner=FlinkRunner --inputFile=pom.xml --output=counts" -Pflink-runner

$> mvn package exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount -Dexec.args="--runner=FlinkRunner --flinkMaster=<flink master> --filesToStage=target/word-count-beam-bundled-0.1.jar --inputFile=/path/to/quickstart/pom.xml --output=/tmp/counts" -Pflink-runner

//You can monitor the running job by visiting the Flink dashboard at http://<flink master>:8081

$> mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount -Dexec.args="--runner=SparkRunner --inputFile=pom.xml --output=counts" -Pspark-runner

//Make sure you complete the setup steps at /documentation/runners/dataflow/#setup

$> mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount -Dexec.args="--runner=DataflowRunner --project=<your-gcp-project> --gcpTempLocation=gs://<your-gcs-bucket>/tmp --inputFile=gs://apache-beam-samples/shakespeare/* --output=gs://<your-gcs-bucket>/counts"  -Pdataflow-runner

$> mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--inputFile=pom.xml --output=/tmp/counts --runner=SamzaRunner" -Psamza-runner

