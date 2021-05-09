## fraud-detection with Flink 

Here we write a flink application to detect suspicious credit card transactions. This example
follows the instructions here https://ci.apache.org/projects/flink/flink-docs-release-1.12/try-flink/datastream_api.html

### Requirements

- Java 8 or 11 
- Maven


### Run project

- clone the repo
- cd into the root (`frauddetection`)
- run `mvn clean package`
- right click on the `pom.xml` file and click `Maven --> reload project`
- run `FraudDetectionJob.main()`

You can also use the flink cluster to run the project, by doing the following:
- make sure Flink is installed https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/try-flink/local_installation/
- cd into the flink folder, e.g. `flink-1.13.01`
- run `./bin/start-cluster.sh` to start the cluster
- run the jar file created by building this project, e.g. `./bin/flink run /path/to/frauddetection/repo/target/frauddetection-0.1.jar`
- monitor the job at `http://localhost:8081/`

![web UI](./images/web_UI.png?raw=true "Flink web UI")