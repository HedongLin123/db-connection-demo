package com.itdl.properties;

import lombok.Data;

import java.io.Serializable;

/**
 * @Description Base JDBC Param
 * @Author itdl
 * @Date 2022/08/10 16:42
 */
@Data
public class BaseJdbcConnParam implements Serializable {

    /**
     * driver name
     */
    private String driverName;

    /**
     * IP
     */
    private String ip;

    /**
     * db server port
     */
    private Integer port;

    /**
     * db name
     */
    private String dbName;

    /**
     * db connection username
     */
    private String username;

    /**
     * db connection password
     */
    private String password;
}
