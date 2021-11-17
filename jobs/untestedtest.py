    def test_remove_non_word_characters(self):
        schema = StructType([
            StructField("person.name", StringType(), True),
            StructField("person", StructType([
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True)]))
        ])
        data = [
            ("charles", Row("chuck", 42)),
            ("lawrence", Row("larry", 73))
        ]
        df = self.spark.createDataFrame(data, schema)
        # df.show()

        cols = ["person", "person.name", "`person.name`"]
        print("***")
        # df.select(cols).show()

        clean_df = df.toDF(*(c.replace('.', '_') for c in df.columns))
        # clean_df.show()
        # clean_df.select("person_name", "person.name", "person.age").show()

    def test_random_values_from_column(self):
        df = self.spark.createDataFrame([(123,), (245,), (12,), (234,)], ['id']).withColumn("rand", F.rand())

        # window = Window.orderBy(df['rand'].desc())
        # actual_df = df.select('*', F.rank().over(window).alias('rank'))\
            # .filter(F.col('rank') <= 3)
        # print(list(map(lambda row: row[0], actual_df.select('id').collect())))
        # actual_df.show()
        # actual_df.explain()

        df2 = df.select('id').orderBy(F.rand()).limit(3)
        # df2.show()
        # df2.explain()

        print(list(map(lambda row: row[0], df.rdd.takeSample(False, 3))))  


    def test_map_to_columns(self):
        data = [("jose", {"a": "aaa", "b": "bbb"}), ("li", {"b": "some_letter", "z": "zed"})]
        df = self.spark.createDataFrame(data, ["first_name", "some_data"])
        # df.withColumn("some_data_a", F.col("some_data")["a"]).show()
        # df.show(truncate=False)
        # df.printSchema()
        df\
            .withColumn("some_data_a", F.col("some_data").getItem("a"))\
            .withColumn("some_data_b", F.col("some_data").getItem("b"))\
            .withColumn("some_data_z", F.col("some_data").getItem("z"))\
            # .show(truncate=False)
        cols = [F.col("first_name")] + list(map(
            lambda f: F.col("some_data").getItem(f).alias(str(f)),
            ["a", "b", "z"]))
        # df.select(cols).show()

        keys_df = df.select(F.explode(F.map_keys(F.col("some_data")))).distinct()
        # keys_df.show()

        keys = list(map(lambda row: row[0], keys_df.collect()))
        # print(keys)

        key_cols = list(map(lambda f: F.col("some_data").getItem(f).alias(str(f)), keys))
        # print(key_cols)

        final_cols = [F.col("first_name")] + key_cols
        # print(final_cols)

        # df.select(final_cols).show()
        # df.select(final_cols).explain(True)


    def test_deep_map_to_columns(self):
        # question inspiration: https://stackoverflow.com/questions/63020351/transform-3-level-nested-dictionary-key-values-to-pyspark-dataframe#63020351
        # schema = StructType([
            # StructField("dic", MapType(StringType(), StringType()), True)]
        # )
        data = [
            ("hi", {"Name": "David", "Age": "25", "Location": "New York", "Height": "170", "fields": {"Color": "Blue", "Shape": "Round", "Hobby": {"Dance": "1", "Singing": "2"}, "Skills": {"Coding": "2", "Swimming": "4"}}}, "bye"),
            ("hi", {"Name": "Helen", "Age": "28", "Location": "New York", "Height": "160", "fields": {"Color": "Blue", "Shape": "Round", "Hobby": {"Dance": "5", "Singing": "6"}}}, "bye"),
            ]
        df = self.spark.createDataFrame(data, ["greeting", "dic", "farewell"])
        res = df.select(
            F.col("dic").getItem("Name").alias(str("Name")),
            F.col("dic")["Age"].alias(str("Age"))
        )
        # res.show()
        # res.printSchema()

        # df.printSchema()

        df.select(F.col("dic").getItem("fields")).printSchema()


    def test_simple_map_to_columns(self):
        # question inpiration: https://stackoverflow.com/questions/36869134/pyspark-converting-a-column-of-type-map-to-multiple-columns-in-a-dataframe
        d = [{'Parameters': {'foo': '1', 'bar': '2', 'baz': 'aaa'}}]
        df = self.spark.createDataFrame(d)
        # df.show(truncate=False)
        # keys_df = df.select(F.explode(F.map_keys(F.col("Parameters")))).distinct()
        # keys = list(map(lambda row: row[0], keys_df.collect()))
        # key_cols = list(map(lambda f: F.col("Parameters").getItem(f).alias(str(f)), keys))
        # df.select(key_cols).show()
        cols = list(map(
            lambda f: F.col("Parameters").getItem(f).alias(str(f)),
            ["foo", "bar", "baz"]))
        # df.select(cols).show()
        # 
        
    def test_pivot(self):
        data = [
            ("123", "McDonalds"),
            ("123", "Starbucks"),
            ("123", "McDonalds"),
            ("777", "McDonalds"),
            ("777", "McDonalds"),
            ("777", "Dunkin")
        ]
        df = self.spark.createDataFrame(data, ["customer_id", "name"])
        df.groupBy("name")#.show()
        # df.groupBy("name").pivot("customer_id").count().show()
        # df.groupBy("customer_id").pivot("name").count().show()

    def test_group_by(self):
        df = self.spark.createDataFrame([[1, 'r1', 1],
            [1, 'r2', 0],
            [1, 'r2', 1],
            [2, 'r1', 1],
            [3, 'r1', 1],
            [3, 'r2', 1],
            [4, 'r1', 0],
            [5, 'r1', 1],
            [5, 'r2', 0],
            [5, 'r1', 1]], schema=['cust_id', 'req', 'req_met'])
        df.groupby(['cust_id', 'req']).agg(F.max(F.col('req_met')).alias('max_req_met'))#.show()
#         