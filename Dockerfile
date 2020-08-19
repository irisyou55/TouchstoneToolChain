FROM openjdk:latest
COPY target/TouchstoneToolchain-0.1.0.jar /TouchstoneToolchain.jar
ENTRYPOINT ["java","-jar","/TouchstoneToolchain-0.1.0.jar"]