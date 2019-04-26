FROM azul/zulu-openjdk:12.0.1 AS builder

COPY . /root/randi

WORKDIR /root/randi

RUN ./mvnw -DskipTests clean install

FROM azul/zulu-openjdk:12.0.1
COPY --from=builder /root/.m2/repository/com/humio/test/randi/0.0.1-SNAPSHOT/randi-0.0.1-SNAPSHOT.jar /root/randi.jar

CMD java -Dspring.profiles.active=random -jar /root/randi.jar