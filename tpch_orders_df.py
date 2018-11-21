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
    .options(table='orders', keyspace=keyspace).load()
    df1.createOrReplaceTempView("orders")
	
    df2 = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='part', keyspace=keyspace).load()
    df2.createOrReplaceTempView("part")
	
    df3 = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='lineitem', keyspace=keyspace).load()
    df3.createOrReplaceTempView("lineitem")
	
    query = "SELECT o.*, p.name FROM orders o JOIN lineitem l ON l.orderkey = o.orderkey \
    JOIN part p ON p.partkey = l.partkey WHERE o.orderkey IN {}".format(orderkeys)
	
    df = spark.sql(query)
    
    '''
	df.createOrReplaceTempView("df_final")
    df_extract = spark.sql("SELECT orderkey, totalprice, STRING_AGG(name, ' ') \
    WITHIN GROUP (ORDER BY name ASC) AS Names FROM df_final GROUP BY orderkey, totalprice")
    df_extract = spark.sql("SELECT orderkey, totalprice, COLLECT_LIST(name) AS names \
	FROM df_final GROUP BY orderkey, totalprice ORDER BY orderkey")
    '''
	
    df_final = df.groupBy(['orderkey','totalprice']).agg(functions.collect_set('name').alias('names')).orderBy('orderkey')
    #df_final.explain()
    rdd1 = df_final.rdd.map(output_line)
    rdd1.saveAsTextFile(outdir) 
 
    
    '''for row in df_extract.rdd.collect(): #here the list has length of the number of orderkeys that we give in our input
        str1 = ""
        for x in sorted(row.names):
            str1 = str1 + x + ", "
        print("Order #{} ${}: {}".format(row.orderkey, round(row.totalprice,2), str1[:-2]))
    
    '''

if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace, outdir, orderkeys)