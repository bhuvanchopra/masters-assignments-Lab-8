import sys
from pyspark.sql import SparkSession, functions, types
from cassandra.cluster import Cluster, BatchStatement, ConsistencyLevel

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra TPCH') \
.config('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
.config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

def main(input_keyspace, output_keyspace):

    df1 = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='orders', keyspace=input_keyspace).load()
    df1.createOrReplaceTempView("orders")
	
    df2 = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='part', keyspace=input_keyspace).load()
    df2.createOrReplaceTempView("part")
	
    df3 = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='lineitem', keyspace=input_keyspace).load()
    df3.createOrReplaceTempView("lineitem")
	
    query = "SELECT o.*, p.name FROM orders o JOIN lineitem l ON \
    l.orderkey = o.orderkey JOIN part p ON p.partkey = l.partkey"	
    df = spark.sql(query)
    
    df4 = df.groupBy(['orderkey']).agg(functions.collect_set('name') \
    .alias('part_names')).orderBy('orderkey')
    	
    df_final = df1.alias('df1').join(df4.alias('df4'), df1.orderkey == df4.orderkey) \
    .select('df1.*', 'df4.part_names').cache()
    df_final = df_final.drop('name')
    
    df_final.write.format("org.apache.spark.sql.cassandra").mode('overwrite') \
    .option('confirm.truncate', True).options(table='orders_parts', keyspace=output_keyspace).save()
	
if __name__ == '__main__':
    input_keyspace = sys.argv[1]
    output_keyspace = sys.argv[2]
    main(input_keyspace, output_keyspace)
