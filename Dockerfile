# Stage 1: Build application with Maven
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Lightweight runtime image
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# Ensure data directory exists for log persistence
RUN mkdir -p /app/data

# Copy shaded runnable jar from builder stage
COPY --from=builder /app/target/simple-kafka-plus-1.0.0.jar app.jar

ENV PORT=8000
ENV DATA_DIR=/app/data

EXPOSE 8000

ENTRYPOINT ["java", "-jar", "app.jar"]
