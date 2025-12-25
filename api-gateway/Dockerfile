FROM gradle:jdk17-ubi9
WORKDIR /project/

COPY ./ ./
EXPOSE 8080

RUN chmod +x ./mvnw
RUN ./mvnw clean package -DskipTests

ENTRYPOINT java -jar target/api-gateway-0.0.1-SNAPSHOT.jar
