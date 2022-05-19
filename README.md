# Java BigQuery Example

Example repository for my blog on [using BigQuery in Java](https://www.sohamkamani.com/java/bigquery/).

To run:

1. Install Java and [Maven](https://maven.apache.org/install.html)
2. call function required in ```main method``` 
3. Within the project directory, run `mvn clean compile assembly:single`
4. To run the application - `GOOGLE_APPLICATION_CREDENTIALS=./test-project-350020-54435eff7a10.json java -jar target/java-bigquery-example-1.0-SNAPSHOT-jar-with-dependencies.jar`