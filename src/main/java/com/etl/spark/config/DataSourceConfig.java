package com.etl.spark.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "spring.datasource")
public class DataSourceConfig {

    public String oracleUrl;
    public String oracleUsername;
    public String oraclePassword;
    public String mysqlUrl;
    public String mysqlUsername;
    public String mysqlPassword;

    public String getOracleUrl() {
        return oracleUrl;
    }

    public void setOracleUrl(String oracleUrl) {
        this.oracleUrl = oracleUrl;
    }

    public String getOracleUsername() {
        return oracleUsername;
    }

    public void setOracleUsername(String oracleUsername) {
        this.oracleUsername = oracleUsername;
    }

    public String getOraclePassword() {
        return oraclePassword;
    }

    public void setOraclePassword(String oraclePassword) {
        this.oraclePassword = oraclePassword;
    }

    public String getMysqlUrl() {
        return mysqlUrl;
    }

    public void setMysqlUrl(String mysqlUrl) {
        this.mysqlUrl = mysqlUrl;
    }

    public String getMysqlUsername() {
        return mysqlUsername;
    }

    public void setMysqlUsername(String mysqlUsername) {
        this.mysqlUsername = mysqlUsername;
    }

    public String getMysqlPassword() {
        return mysqlPassword;
    }

    public void setMysqlPassword(String mysqlPassword) {
        this.mysqlPassword = mysqlPassword;
    }
}
