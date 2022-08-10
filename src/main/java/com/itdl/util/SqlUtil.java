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


}
