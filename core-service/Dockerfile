FROM gradle:jdk17-ubi9
WORKDIR /project/

COPY ./ ./
EXPOSE 8081

RUN chmod +x ./mvnw
RUN ./mvnw clean package -DskipTests

ENTRYPOINT java -jar target/core-service-0.0.1-SNAPSHOT.jar
