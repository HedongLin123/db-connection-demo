# SpringBoot整合Hive(开启Kerberos认证)作为三方数据源管理

## Hive数据库连接说明

* 1、没有开启kerberos认证，需要正常的jdbc url, 账号+密码就能获取到Connection
* 2、开启了kerberos认证，不需要密码，需要密钥文件(kertab文件)，认证配置文件(kbr5文件)
* 3、这两个文件从哪儿来，由Hive数据库的管理员哪儿获取

## 开启Kerberos认证后连接遇到的坑

* 1、直接认证不通过，一般是账户，kbr5文件，kertab文件错误
* 2、认证成功，但是获取不到连接，发现使用IP连接，但是kbr5文件配置的是域名，认证不成功，统一使用域名解决
* 3、获取连接成功，但是执行SQL失败。发现是Hive的数据库名错了，它也能连接，只是库下面没有表而已


## 编写HiveJdbc连接参数类

包含了JDBC连接基础类，后期还会集成Oracle，Mysql, MaxCompute, Dataworks(前面文章已经集成)等数据源


```java
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

/**
 * @Description Hive JDBC connection params
 * @Author itdl
 * @Date 2022/08/10 16:40
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class HiveJdbcConnParam extends BaseJdbcConnParam {
    /**
     * enable kerberos authentication
     */
    private boolean enableKerberos;

    /**
     * principal
     */
    private String principal;

    /**
     * kbr5 file path in dick
     */
    private String kbr5FilePath;

    /**
     * keytab file path in dick
     */
    private String keytabFilePath;
}


```

## 编写Hive连接工具类

主要用于获取Hive的连接，包括普通连接和基于Kerberos认证的连接

```java
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
```

## 编写Sql操作工具类

用于根据连接去执行SQL，测试时候使用，正常整合三方数据源时作为执行三方数据源的SQL语句操作的工具类

```java
package com.itdl.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.itdl.common.base.ResultCode;
import com.itdl.exception.BizException;
import com.itdl.properties.HiveJdbcConnParam;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @Description SQL执行工具类
 * @Author itdl
 * @Date 2022/08/10 17:13
 */
public class SqlUtil {

    private final Connection connection;

    public SqlUtil(Connection connection) {
        this.connection = connection;
    }

    public static SqlUtil build(Connection connection){
        return new SqlUtil(connection);
    }

    /**
     * 执行SQL查询
     * @param sql sql语句
     * @return 数据列表，使用LinkedHashMap是为了防止HashMap序列化后导致顺序乱序
     */
    public List<LinkedHashMap<String, Object>> querySql(String sql){
        // 执行sql
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            return buildListMap(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BizException(ResultCode.SQL_EXEC_ERR.getCode(), e.getMessage());
        }finally {
            // 关闭
            close(resultSet, statement);
        }
    }


    /**
     * 关闭对象 传入多个时注意顺序， 需要先关闭哪个就传在参数前面
     * @param objs 对象动态数组
     */
    private void close(Object ...objs){
        if (objs == null || objs.length == 0){
            return;
        }

        for (Object obj : objs) {
            if (obj instanceof Statement){
                try {
                    ((Statement) obj).close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }

            if (obj instanceof ResultSet){
                try {
                    ((ResultSet) obj).close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }

            if (obj instanceof Connection){
                try {
                    ((Connection) obj).close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * @Description 功能描述：将resultSet构造为List<Map>
     * @Author itdl
     * @Date 2022/4/18 21:13
     * @Param {@link ResultSet} resultSet
     * @Return {@link List < Map <String,Object>>}
     **/
    private List<LinkedHashMap<String, Object>> buildListMap(ResultSet resultSet) throws SQLException {
        if (resultSet == null) {
            return Lists.newArrayList();
        }

        List<LinkedHashMap<String, Object>> resultList = new ArrayList<>();
        // 获取元数据
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            // 获取列数
            int columnCount = metaData.getColumnCount();
            LinkedHashMap<String, Object> map = new LinkedHashMap<>();
            for (int i = 0; i < columnCount; i++) {
                String columnName = metaData.getColumnName(i + 1);
                // 过滤掉查询的结果包含序号的
                if("mm.row_num_01".equalsIgnoreCase(columnName)
                        || "row_num_01".equalsIgnoreCase(columnName)){
                    continue;
                }

                // 去除hive查询结果的mm.别名前缀
                if (columnName.startsWith("mm.")){
                    columnName = columnName.substring(columnName.indexOf(".") + 1);
                }

                Object object = resultSet.getObject(columnName);
                map.put(columnName, object);
            }

            resultList.add(map);
        }
        return resultList;
    }
}

```


## 测试方法

```java
public static void main(String[] args) {
    final HiveJdbcConnParam connParam = new HiveJdbcConnParam();
    connParam.setDriverName("org.apache.hive.jdbc.HiveDriver");
    connParam.setIp("IP或者域名");
    connParam.setPort(10000);
    connParam.setDbName("数据库名");
    // 开启kerbers的账号格式一般为  用户名@域名
    connParam.setUsername("账号");
    // 开启kerberos后 不需要密码了
    connParam.setPassword("1212121221");
    // 是否开启kerberos认证
    connParam.setEnableKerberos(true);
    // 凭证，也就是跟在jdbc url后的;principle=的那一段
    connParam.setPrincipal("库名/主机@域名");
    // kbr5认证配置文件路径 注意：里面是域名，那么连接的时候也是域名
    connParam.setKbr5FilePath("C:\\workspace\\krb5.conf");
    // 密钥文件路径 用于认证验证
    connParam.setKeytabFilePath("C:\\workspace\\用户名.keytab");

    final Connection connection = new HiveConnUtil(connParam).getConnection();
    final SqlUtil sqlUtil = SqlUtil.build(connection);
    final List<LinkedHashMap<String, Object>> tables = sqlUtil.querySql("show databases");
    for (LinkedHashMap<String, Object> table : tables) {
        final String s = JSONObject.toJSONString(table);
        System.out.println(s);
    }

    sqlUtil.close(connection);
}
```

连接都拿到了，也能执行SQL了，工具类也有了，做一个三方数据源管理还有什么能难道天才般的你呢？

## 测试日志
```shell
18:04:14.719 [main] DEBUG org.apache.hadoop.metrics2.lib.MutableMetricsFactory - field org.apache.hadoop.metrics2.lib.MutableRate org.apache.hadoop.security.UserGroupInformation$UgiMetrics.loginSuccess with annotation @org.apache.hadoop.metrics2.annotation.Metric(always=false, sampleName=Ops, about=, type=DEFAULT, value=[Rate of successful kerberos logins and latency (milliseconds)], valueName=Time)
18:04:14.729 [main] DEBUG org.apache.hadoop.metrics2.lib.MutableMetricsFactory - field org.apache.hadoop.metrics2.lib.MutableRate org.apache.hadoop.security.UserGroupInformation$UgiMetrics.loginFailure with annotation @org.apache.hadoop.metrics2.annotation.Metric(always=false, sampleName=Ops, about=, type=DEFAULT, value=[Rate of failed kerberos logins and latency (milliseconds)], valueName=Time)
18:04:14.729 [main] DEBUG org.apache.hadoop.metrics2.lib.MutableMetricsFactory - field org.apache.hadoop.metrics2.lib.MutableRate org.apache.hadoop.security.UserGroupInformation$UgiMetrics.getGroups with annotation @org.apache.hadoop.metrics2.annotation.Metric(always=false, sampleName=Ops, about=, type=DEFAULT, value=[GetGroups], valueName=Time)
18:04:14.736 [main] DEBUG org.apache.hadoop.metrics2.impl.MetricsSystemImpl - UgiMetrics, User and group related metrics
18:04:14.796 [main] DEBUG org.apache.hadoop.security.Groups -  Creating new Groups object
18:04:14.799 [main] DEBUG org.apache.hadoop.util.NativeCodeLoader - Trying to load the custom-built native-hadoop library...
18:04:14.802 [main] DEBUG org.apache.hadoop.util.NativeCodeLoader - Failed to load native-hadoop with error: java.lang.UnsatisfiedLinkError: C:\workspace\software\hadoop\winutils\hadoop-3.0.1\bin\hadoop.dll: Can't load AMD 64-bit .dll on a IA 32-bit platform
18:04:14.803 [main] DEBUG org.apache.hadoop.util.NativeCodeLoader - java.library.path=C:\Program Files (x86)\Java\jdk1.8.0_271\bin;C:\windows\Sun\Java\bin;C:\windows\system32;C:\windows;C:\Program Files (x86)\Common Files\Oracle\Java\javapath;C:\windows\system32;C:\windows;C:\windows\System32\Wbem;C:\windows\System32\WindowsPowerShell\v1.0\;C:\windows\System32\OpenSSH\;C:\Program Files\Docker\Docker\resources\bin;C:\ProgramData\DockerDesktop\version-bin;C:\Program Files (x86)\NetSarang\Xshell 7\;C:\Program Files (x86)\NetSarang\Xftp 7\;C:\Program Files\TortoiseGit\bin;C:\Program Files\MIT\Kerberos\bin;C:\workspace\software\python\Scripts\;C:\workspace\software\python\;C:\Users\donglin.he\AppData\Local\Programs\Python\Launcher\;C:\Users\donglin.he\AppData\Local\Microsoft\WindowsApps;C:\workspace\software\Git\cmd;C:\workspace\software\maven\apache-maven-3.6.3\bin;C:\Program Files (x86)\Java\jdk1.8.0_271\bin;C:\workspace\software\PyCharm 2022.1.2\bin;;C:\workspace\software\hadoop\winutils\hadoop-3.0.1\bin;C:\workspace\software\python;C:\workspace\software\python\Scripts;;.
18:04:14.803 [main] WARN org.apache.hadoop.util.NativeCodeLoader - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18:04:14.803 [main] DEBUG org.apache.hadoop.util.PerformanceAdvisory - Falling back to shell based
18:04:14.803 [main] DEBUG org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback - Group mapping impl=org.apache.hadoop.security.ShellBasedUnixGroupsMapping
18:04:14.876 [main] DEBUG org.apache.hadoop.security.Groups - Group mapping impl=org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback; cacheTimeout=300000; warningDeltaMs=5000
18:04:15.626 [main] DEBUG org.apache.hadoop.security.UserGroupInformation - hadoop login
18:04:15.627 [main] DEBUG org.apache.hadoop.security.UserGroupInformation - hadoop login commit
18:04:15.628 [main] DEBUG org.apache.hadoop.security.UserGroupInformation - using kerberos user:你的用户名
18:04:15.628 [main] DEBUG org.apache.hadoop.security.UserGroupInformation - Using user: "你的用户名" with name 你的用户名
18:04:15.628 [main] DEBUG org.apache.hadoop.security.UserGroupInformation - User entry: "你的用户名"
18:04:15.628 [main] INFO org.apache.hadoop.security.UserGroupInformation - Login successful for user 你的用户名 using keytab file C:\workspace\zhouyu.keytab
18:04:15.641 [main] INFO org.apache.hive.jdbc.Utils - Supplied authorities: Hive的域名:10000
18:04:15.642 [main] INFO org.apache.hive.jdbc.Utils - Resolved authority: Hive的域名:10000
18:04:15.656 [main] DEBUG org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge - Current authMethod = KERBEROS
18:04:15.656 [main] DEBUG org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge - Not setting UGI conf as passed-in authMethod of kerberos = current.
18:04:15.678 [main] DEBUG org.apache.hadoop.security.UserGroupInformation - PrivilegedAction as:你的用户名 (auth:KERBEROS) from:org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport.open(TUGIAssumingTransport.java:49)
18:04:15.678 [main] DEBUG org.apache.thrift.transport.TSaslTransport - opening transport org.apache.thrift.transport.TSaslClientTransport@1b9f5a4
18:04:15.812 [main] DEBUG org.apache.thrift.transport.TSaslClientTransport - Sending mechanism name GSSAPI and initial response of length 567
18:04:15.819 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: Writing message with status START and payload length 6
18:04:15.820 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: Writing message with status OK and payload length 567
18:04:15.820 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: Start message handled
18:04:15.952 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: Received message with status OK and payload length 104
18:04:15.954 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: Writing message with status OK and payload length 0
18:04:15.993 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: Received message with status OK and payload length 50
18:04:15.994 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: Writing message with status COMPLETE and payload length 50
18:04:15.994 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: Main negotiation loop complete
18:04:15.994 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: SASL Client receiving last message
18:04:16.034 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: Received message with status COMPLETE and payload length 0
18:04:16.053 [main] DEBUG org.apache.thrift.transport.TSaslTransport - writing data length: 67
18:04:16.155 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: reading data length: 109
18:04:16.239 [main] INFO com.itdl.util.HiveConnUtil - =====>>>获取hive连接成功：username:你的用户名,jdbcUrl: jdbc:hive2://Hive的域名:10000/库名;principal=hive/你的principle
18:04:16.247 [main] DEBUG org.apache.thrift.transport.TSaslTransport - writing data length: 132
18:04:35.551 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: reading data length: 109
18:04:35.560 [main] DEBUG org.apache.thrift.transport.TSaslTransport - writing data length: 100
18:04:35.618 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: reading data length: 255
18:04:35.629 [main] DEBUG org.apache.thrift.transport.TSaslTransport - writing data length: 102
18:04:35.668 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: reading data length: 136
18:04:35.699 [main] DEBUG org.apache.thrift.transport.TSaslTransport - writing data length: 112
18:04:35.755 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: reading data length: 325
18:04:35.775 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.hive.jdbc.HiveQueryResultSet - Fetched row string: 
18:04:35.776 [main] DEBUG org.apache.thrift.transport.TSaslTransport - writing data length: 112
18:04:35.816 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: reading data length: 96
18:04:35.820 [main] DEBUG org.apache.thrift.transport.TSaslTransport - writing data length: 96
18:04:35.873 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: reading data length: 42
{"database_name":"db_01"}
{"database_name":"db_02"}
{"database_name":"db_03"}
{"database_name":"db_04"}
{"database_name":"communication_bank"}
18:04:35.965 [main] DEBUG org.apache.thrift.transport.TSaslTransport - writing data length: 83
18:04:36.007 [main] DEBUG org.apache.thrift.transport.TSaslTransport - CLIENT: reading data length: 40
```

## 项目地址





