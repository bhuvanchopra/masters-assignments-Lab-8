import sys
from pyspark.sql import SparkSession, functions, types
from cassandra.cluster import Cluster, BatchStatement, ConsistencyLevel

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra Logs') \
.config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

def main(keyspace, table):

    df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table=table, keyspace=keyspace).load().cache()

    df1 = df.groupBy('host').agg(functions.count('bytes').alias('x'))
    df2 = df.groupBy('host').agg(functions.sum('bytes').alias('y'))
    df_both = df1.join(df2, 'host').cache()
    df_final = df_both.select('host',functions.lit(1).alias('1'),'x','y',(df_both.x**2).alias('x2'), \
    (df_both.y**2).alias('y2'),(df_both.x*df_both.y).alias('xy'))
    
    sums = df_final.groupBy().sum().cache()
    list1 = list(sums.first())
    
    r = ((list1[0]*list1[5]) - (list1[1]*list1[2]))/((((list1[0]*list1[3]) - list1[1]**2)**0.5)* \
    (((list1[0]*list1[4]) - list1[2]**2)**0.5))
    print('r = %.6f' % r)
    print('r^2 = %.6f' % r**2)

if __name__ == '__main__':
    keyspace = sys.argv[1]
    table = sys.argv[2]
    main(keyspace, table)
