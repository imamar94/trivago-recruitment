FROM bitnami/spark:3.4.3

USER root

# Install python deps
COPY requirements.txt .
RUN pip3 install -r requirements.txt

EXPOSE 8080
EXPOSE 7070

USER 1001
