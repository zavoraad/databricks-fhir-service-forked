FROM eclipse-temurin:17-jre

WORKDIR /app

# Copy the assembly JAR
COPY target/scala-3.4.2/databricks-fhir-service.jar /app/databricks-fhir-service.jar

EXPOSE 9000

# Required for Databricks JDBC / Apache Arrow (MemoryUtil) on Java 9+
ENTRYPOINT ["java", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-jar", "/app/databricks-fhir-service.jar"]
