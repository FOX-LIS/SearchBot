FROM openjdk:17-alpine
MAINTAINER Lisunov Vladimir
COPY target/SearchBot-0.0.1-SNAPSHOT.jar searchbot.jar
ENTRYPOINT ["java", "-jar", "/searchbot.jar"]