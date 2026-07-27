FROM eclipse-temurin:11-jre
ARG SPLINE_SERVER_VERSION=1.0.0-RC3
RUN curl -fsSL -o /opt/spline-admin.jar \
  "https://repo1.maven.org/maven2/za/co/absa/spline/admin/${SPLINE_SERVER_VERSION}/admin-${SPLINE_SERVER_VERSION}.jar"
ENTRYPOINT ["java","-jar","/opt/spline-admin.jar"]