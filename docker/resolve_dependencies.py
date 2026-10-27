"""Build-time only: verify dependencies, then include their JARs in the Spark image."""
import json
from pathlib import Path
import shutil
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('week4-build-dependencies').getOrCreate()
try:
    # Force loading of the two runtime integrations before publishing the image.
    loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    for name in ('org.apache.spark.sql.kafka010.KafkaSourceProvider', 'org.postgresql.Driver'):
        loader.loadClass(name)
finally:
    spark.stop()

jars = sorted(Path('/tmp/week4-build-ivy/jars').glob('*.jar'))
if not jars:
    raise RuntimeError('No resolved dependencies found during image build')
installed = []
for jar in jars:
    # Ivy prefixes group IDs; retain conventional artifact filenames in SPARK_HOME.
    name = jar.name.split('_', 1)[1]
    target = Path('/opt/spark/jars') / name
    if not target.exists():
        shutil.copyfile(jar, target)
    installed.append(name)
Path('/opt/spark/week4-dependencies.json').write_text(json.dumps(installed, indent=2) + '\n')
print('Bundled week4 dependencies:', ', '.join(installed))
