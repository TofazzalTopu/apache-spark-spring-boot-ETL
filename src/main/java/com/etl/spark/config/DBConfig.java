package com.etl.spark.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DBConfig {

    @Value("${spring.datasource.oracle.url}")
    private String oracleUrl;

    @Value("${spring.datasource.oracle.username}")
    private String oracleUser;

    @Value("${spring.datasource.oracle.password}")
    private String oraclePassword;

    @Value("${spring.datasource.mysql.url}")
    private String mysqlUrl;

    @Value("${spring.datasource.mysql.username}")
    private String mysqlUser;

    @Value("${spring.datasource.mysql.password}")
    private String mysqlPassword;

    public String getOracleUrl() { return oracleUrl; }
    public String getOracleUser() { return oracleUser; }
    public String getOraclePassword() { return oraclePassword; }
    public String getMySQLUrl() { return mysqlUrl; }
    public String getMySQLUser() { return mysqlUser; }
    public String getMySQLPassword() { return mysqlPassword; }
}

