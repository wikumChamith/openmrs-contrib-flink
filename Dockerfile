# Multi-stage build for Spring Boot application
FROM gradle:8.5-jdk21 AS build

WORKDIR /app

# Copy gradle files first for better caching
COPY build.gradle settings.gradle ./
COPY gradle ./gradle

# Download dependencies
RUN gradle dependencies --no-daemon || true

# Copy source code
COPY src ./src

# Build the application
RUN gradle bootJar --no-daemon -x test

# Extract the fat jar in the build stage (JDK has the jar tool).
# This is required because Flink's mini-cluster spawns threads that use the
# system classloader, which cannot access classes nested inside Spring Boot's
# BOOT-INF/lib/ structure.
RUN mkdir -p /app/unpacked && cd /app/unpacked && jar xf /app/build/libs/*.jar

# Runtime stage
FROM eclipse-temurin:21-jre

WORKDIR /app

# Copy extracted jar contents from build stage
COPY --from=build /app/unpacked /app/unpacked

# Expose application port
EXPOSE 8081

# Run with explicit classpath instead of java -jar
ENTRYPOINT ["java", "-cp", "/app/unpacked/BOOT-INF/classes:/app/unpacked/BOOT-INF/lib/*", "com.openmrs.OpenmrsContribFlinkApplication"]