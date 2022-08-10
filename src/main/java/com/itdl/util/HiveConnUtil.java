package com.itdl.util;

import com.itdl.common.base.ResultCode;
import com.itdl.exception.BizException;
import com.itdl.properties.HiveJdbcConnParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Description hive connection util
 * @Author itdl
 * @Date 2022/08/10 16:52
 */
@Slf4j
public class HiveConnUtil {
    /**
     * connection params
     */
    private final HiveJdbcConnParam connParam;

    /**
     * jdbc connection object
     */
    private final Connection connection;

    public HiveConnUtil(HiveJdbcConnParam connParam) {
        this.connParam = connParam;
        this.connection = buildConnection();
    }

    /**
     * 获取连接
     * @return 连接
     */
    public Connection getConnection() {
        return connection;
    }

    private Connection buildConnection(){
        try {
//            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Class.forName(connParam.getDriverName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new BizException(ResultCode.HIVE_DRIVE_LOAD_ERR);
        }
        // 开启kerberos后需要私钥
        // 拼接jdbcUrl
        String jdbcUrl = "jdbc:hive2://%s:%s/%s";
        String ip = connParam.getIp();
        String port = connParam.getPort() + "";
        String dbName = connParam.getDbName();
        final String username = connParam.getUsername();
        final String password = connParam.getPassword();
        // is enable kerberos authentication
        final boolean enableKerberos = connParam.isEnableKerberos();
        // 格式化
        Connection connection;
        // 获取连接
        try {
            if (!enableKerberos) {
                jdbcUrl = String.format(jdbcUrl, ip, port, dbName);
                connection = DriverManager.getConnection(jdbcUrl, username, password);
            } else {
                final String principal = connParam.getPrincipal();
                final String kbr5FilePath = connParam.getKbr5FilePath();
                final String secretFilePath = connParam.getKeytabFilePath();

                String format = "jdbc:hive2://%s:%s/%s;principal=%s";
                jdbcUrl = String.format(format, ip, port, dbName, principal);

                // 使用hadoop安全认证
                System.setProperty("java.security.krb5.conf", kbr5FilePath);
                System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
                // 解决windows中执行可能出现找不到HADOOP_HOME或hadoop.home.dir问题
                // Kerberos认证
                org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                conf.set("hadoop.security.authentication", "Kerberos");
                conf.set("keytab.file", secretFilePath);
                conf.set("kerberos.principal", principal);
                UserGroupInformation.setConfiguration(conf);
                try {
                    UserGroupInformation.loginUserFromKeytab(username, secretFilePath);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new BizException(ResultCode.KERBEROS_AUTH_FAIL_ERR);
                }
                try {
                    connection = DriverManager.getConnection(jdbcUrl);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BizException(ResultCode.KERBEROS_AUTH_SUCCESS_GET_CONN_FAIL_ERR);
                }
            }
            log.info("=====>>>获取hive连接成功：username:{},jdbcUrl: {}", username, jdbcUrl);
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BizException(ResultCode.HIVE_CONN_USER_PWD_ERR);
        } catch (BizException e){
            throw e;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new BizException(ResultCode.HIVE_CONN_ERR);
        }
    }

}
