content = "How happy I am that I am gone"
rdd = sc.parallelize(content.split(' '))\
    .map(lambda x: (x, 1))\
    .reduceByKey(lambda x,y: x + y)

rdd.toDF(['word','count'])\
    .orderBy('count')\
    .show()