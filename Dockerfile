FROM eclipse-temurin:17-jre

WORKDIR /app

COPY target/transfer-service.jar app.jar

# Настройки для разработки
ENV JAVA_TOOL_OPTIONS="-Xmx512m -Xms256m"
ENV SPRING_PROFILES_ACTIVE="docker"

EXPOSE 8085

ENTRYPOINT ["java", "-jar", "app.jar"]