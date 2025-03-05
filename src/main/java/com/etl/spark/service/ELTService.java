package com.etl.spark.service;

import com.etl.spark.config.CustomJdbcReader;
import com.etl.spark.config.DataSourceConfig;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ELTService {
    private static final Logger logger = LoggerFactory.getLogger(ELTService.class);

    private final CustomJdbcReader oracleReader;
    private final CustomJdbcReader mysqlReader;

    @Autowired
    public ELTService(DataSourceConfig databaseConfig) {
        SparkSession spark = SparkSession.builder()
                .appName("ELT Process")
                .master("local[*]")
                .getOrCreate();

        this.oracleReader = new CustomJdbcReader(
                spark,
                databaseConfig.getOracleUrl(),
                databaseConfig.getOracleUsername(),
                databaseConfig.getOraclePassword()
        );
        this.mysqlReader = new CustomJdbcReader(
                spark,
                databaseConfig.getMysqlUrl(),
                databaseConfig.getMysqlUsername(),
                databaseConfig.getMysqlPassword()
        );
    }


    public void runELTProcess() {
        try {
            logger.info("Spark session initialized.");
            String oracleTable = "your_oracle_table";
            String mysqlTable = "your_mysql_table";
            // Fetch data from Oracle
            Dataset<Row> oracleData = oracleReader.loadTable(oracleTable)
                    .filter("id > " + oracleReader.maxId(oracleTable));
            logger.info("Data extracted from Oracle.");

            // Transformation logic
            Dataset<Row> transformedData = oracleData.filter("column_name > 100");
            transformedData.cache(); // Cache for reuse
            logger.info("Data transformation completed.");

            // Load data into MySQL
            mysqlReader.writeTable(transformedData, mysqlTable, "overwrite");
            logger.info("Data loaded into MySQL.");
        } catch (Exception e) {
            logger.error("Error occurred during ELT process: {}", e.getMessage());
        }
    }
}