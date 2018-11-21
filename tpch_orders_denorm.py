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

def output_line(line):
    namestr = ', '.join(sorted(list(line[2])))
    return 'Order #%d $%.2f: %s' % (line[0], line[1], namestr)

def main(keyspace, outdir, orderkeys):

    orderkeys = tuple([int(x) for x in orderkeys])
   
    df1 = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='orders_parts', keyspace=keyspace).load()
    df1.createOrReplaceTempView("orders_parts")
	
    query = "SELECT orderkey, totalprice, part_names FROM orders_parts \
    WHERE orderkey IN {}".format(orderkeys)
	
    df_final = spark.sql(query).orderBy('orderkey')
        
    rdd1 = df_final.rdd.map(output_line)
    rdd1.saveAsTextFile(outdir) 
    
if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace, outdir, orderkeys)