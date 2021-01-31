FROM adoptopenjdk:8-jdk-openj9 AS GRADLE_BUILD
RUN mkdir -p ./build/libs/
RUN curl -L http://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-jvm/1.6.0/jolokia-jvm-1.6.0-agent.jar -o ./build/libs/jolokia-jvm-agent.jar
COPY ./ ./
RUN export $(cat env/sample.properties | xargs -L1) && ./gradlew build

FROM openjdk:8-jre-alpine
COPY --from=GRADLE_BUILD ./build/libs/ /opt/beast
WORKDIR /opt/beast
CMD ["java", "-cp", "./*", "-javaagent:jolokia-jvm-agent.jar=port=7777,host=0.0.0.0", "com.gojek.beast.launch.Main"]