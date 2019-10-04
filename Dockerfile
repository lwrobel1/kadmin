FROM openjdk:8-jdk as builder
WORKDIR /app
COPY . .
RUN ./gradlew clean build -x test

FROM openjdk:8-jre
WORKDIR /app

COPY --from=builder /app/build/libs/app-boot.jar /app/app.jar
COPY application.properties /app/application.properties

EXPOSE 8080

ENTRYPOINT [ "java", "-jar", "/app/app.jar" , "--spring.profiles.active=kadmin,local", "--server.contextPath=/"]