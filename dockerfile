FROM openjdk:18-jdk
COPY target/kafkaproducer-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar
ENTRYPOINT ["java","-jar","/app.jar"]