package com.etl.spark.config;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CustomJdbcReader {

    private final SparkSession spark;
    private final String url;
    private final String username;
    private final String password;

    public CustomJdbcReader(SparkSession spark, String url, String username, String password) {
        this.spark = spark;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    // Method to get a configured DataFrameReader
    public DataFrameReader getReader() {
        return spark.read()
                .format("jdbc")
                .option("url", url)
                .option("user", username)
                .option("password", password);
    }

    // Method to load a table
    public Dataset<Row> loadTable(String tableName) {
        return getReader()
                .option("dbtable", tableName)
                .load();
    }

    public long maxId(String tableName) {
        // Fetch the maximum ID from the target MySQL table
        String maxIdQuery = "SELECT MAX(id) FROM " + tableName + "";
        long maxId = getReader()
                .option("dbtable", "(" + maxIdQuery + ") AS max_id")
                .load()
                .collectAsList()
                .get(0)
                .getLong(0);
        return maxId;
    }

    // Method to write data to a table
    public void writeTable(Dataset<Row> data, String tableName, String mode) {
        data.write()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", tableName)
                .option("user", username)
                .option("password", password)
                .mode(mode)
                .save();
    }
}
