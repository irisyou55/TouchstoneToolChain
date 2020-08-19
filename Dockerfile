FROM openjdk:latest
COPY TouchstoneToolchain-0.1.0.jar /TouchstoneToolchain-0.1.0.jar
ENTRYPOINT ["java","-jar","/TouchstoneToolchain-0.1.0.jar"]