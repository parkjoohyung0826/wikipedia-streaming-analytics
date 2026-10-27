FROM spark:3.5.8-python3
USER root
RUN pip3 install --no-cache-dir psycopg2-binary==2.9.10
COPY docker/ivysettings.xml /opt/week4-ivysettings.xml
COPY docker/resolve_dependencies.py /opt/week4-resolve.py
# Resolve all transitive dependencies while building, on the build network.
# A Spark job must never need external Maven DNS at runtime.
RUN SPARK_LOCAL_IP=127.0.0.1 /opt/spark/bin/spark-submit \
    --master 'local[1]' --driver-memory 512m \
    --conf spark.jars.ivy=/tmp/week4-build-ivy \
    --conf spark.jars.ivySettings=/opt/week4-ivysettings.xml \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,org.postgresql:postgresql:42.7.7 \
    /opt/week4-resolve.py
USER spark
