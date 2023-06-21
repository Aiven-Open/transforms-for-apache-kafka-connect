FROM eclipse-temurin:11-jdk AS base

ADD ./ transforms
WORKDIR transforms

RUN ./gradlew installDist

FROM confluentinc/cp-kafka-connect:7.3.3

COPY --from=base /transforms/build/install/transforms-for-apache-kafka-connect /transforms
