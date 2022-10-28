import pyspark

def test_pyspark_install():
    sc = pyspark.SparkContext('local[*]', "PySpark install test")