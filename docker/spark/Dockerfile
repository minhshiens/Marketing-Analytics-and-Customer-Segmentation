FROM apache/spark:3.5.1
USER root
RUN apt-get update && apt-get install -y curl
RUN curl -L -o /opt/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar