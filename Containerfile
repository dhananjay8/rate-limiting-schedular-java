FROM docker.io/library/gradle:8.13-jdk17 AS build
WORKDIR /workspace
COPY . .
RUN gradle --no-daemon clean bootJar

FROM docker.io/library/eclipse-temurin:17-jre
LABEL org.opencontainers.image.title="Rate-Limiting Schedular Service"
LABEL org.opencontainers.image.version="0.1.0-SNAPSHOT"
LABEL org.opencontainers.image.description="Kafka and Redis based rate-limited task scheduler"
WORKDIR /app
COPY --from=build /workspace/build/libs/rate-limiting-schedular-service-0.1.0-SNAPSHOT.jar /app/service.jar
USER 10001:10001
EXPOSE 8080
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/app/service.jar"]
