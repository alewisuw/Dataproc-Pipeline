package com.google.cloud.spark;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("StackOverflow Pipeline").master("local[*]").config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
        // GS PATH
        // Dataset<Row> df = spark.read().parquet("gs://dataeng_stackoverflow_data/data/000000000000.parquet");

        Dataset<Row> df = spark.read().parquet("data//data_0.parquet"); //example with a fraction of the data

        df = df.withColumn("year", functions.year(df.col("creation_date")));

        df = df.withColumn("language", 
            functions.expr("CASE " +
                "WHEN lower(title) LIKE '%java%' THEN 'Java' " +
                "WHEN lower(title) LIKE '%python%' THEN 'Python' " +
                "WHEN lower(title) LIKE '%javascript%' THEN 'JavaScript' " +
                "WHEN lower(title) LIKE '%c++%' THEN 'C++' " +
                "WHEN lower(title) LIKE '%c#%' THEN 'C#' " +
                "WHEN lower(title) LIKE '%ruby%' THEN 'Ruby' " +
                "WHEN lower(title) LIKE '%go%' THEN 'Go' " +
                "WHEN lower(title) LIKE '%swift%' THEN 'Swift' " +
                "WHEN lower(title) LIKE '%kotlin%' THEN 'Kotlin' " +
                "WHEN lower(title) LIKE '%r%' THEN 'R' " +
                "ELSE NULL END"));  

        df = df.filter(df.col("language").isNotNull());

        df = df.groupBy("year", "language").agg(functions.sum("view_count").alias("total_views"));

        df.show();
    }
}