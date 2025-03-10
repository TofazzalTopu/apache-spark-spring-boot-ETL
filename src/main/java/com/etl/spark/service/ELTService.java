package com.etl.spark.service;

import com.etl.spark.config.CustomJdbcReader;
import com.etl.spark.config.DBConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ELTService {

    private static final Logger logger = LoggerFactory.getLogger(ELTService.class);

    private final CustomJdbcReader oracleReader;
    private final CustomJdbcReader mysqlReader;
    private final SparkSession spark;

    @Autowired
    public ELTService(DBConfig dbConfig) {
        this.spark = SparkSession.builder()
                .appName("ELT Process")
                .master("local[*]")
                .getOrCreate();

        this.oracleReader = new CustomJdbcReader(
                spark,
                dbConfig.getOracleUrl(),
                dbConfig.getOracleUser(),
                dbConfig.getOraclePassword()
        );

        this.mysqlReader = new CustomJdbcReader(
                spark,
                dbConfig.getMySQLUrl(),
                dbConfig.getMySQLUser(),
                dbConfig.getMySQLPassword()
        );
        logger.info("dataSourceConfig.getMysqlUrl() = {}", dbConfig.getMySQLUser());
    }


    public void runELTProcess() {
        try {
            String oracleTable = "products";
            String mysqlTable = "products_test";
            // Fetch data from Oracle
//            Dataset<Row> oracleData = oracleReader.loadTable(oracleTable)
//                    .filter("id > " + oracleReader.maxId(oracleTable));
            Dataset<Row> mysqlDbRecords = mysqlReader.loadTable(oracleTable).filter("id > 2");
            // Transformation logic
//            Dataset<Row> transformedData = oracleData.filter("id < 5");
            Dataset<Row> transformedData = mysqlDbRecords;
            transformedData.cache(); // Cache for reuse

            // Load data into MySQL
            mysqlReader.writeTable(transformedData, mysqlTable, "overwrite");
        } catch (Exception e) {
            logger.info("Error running ELT process: {} ", e.getMessage());
        }
    }
}