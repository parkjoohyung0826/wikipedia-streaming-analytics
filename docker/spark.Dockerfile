FROM spark:3.5.8-python3
USER root
RUN pip3 install --no-cache-dir psycopg2-binary==2.9.10
USER spark
