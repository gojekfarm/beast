FROM openjdk:8-jre-alpine

ADD ./build/libs/ /opt/beast

WORKDIR /opt/beast
CMD ["java", "-cp", "./*", "-javaagent:jolokia-jvm-agent.jar=port=7777,host=0.0.0.0", "com.gojek.beast.launch.Main"]

