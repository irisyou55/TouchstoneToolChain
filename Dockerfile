FROM openjdk:latest
COPY target/TouchstoneToolchain-*.jar /TouchstoneToolchain.jar
ENTRYPOINT ["java","-jar","/TouchstoneToolchain.jar"]