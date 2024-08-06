package com.google.cloud.spark;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("test sql").master("local[*]").config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
        Dataset<Row> parquetFileDF = spark.read().parquet("gs://dataeng_stackoverflow_data/data/000000000000.parquet");
    }
}