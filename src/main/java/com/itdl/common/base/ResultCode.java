package com.itdl.common.base;

import com.itdl.common.enums.BaseEnums;
import lombok.Getter;

/**
 * @Description
 * @Author itdl
 * @Date 2022/08/08 10:40
 */
@Getter
public enum ResultCode implements BaseEnums<String, String> {
    /**通用业务返回码定义，系统-编码*/
    SUCCESS("db-conn-000000", "success"),


    KERBEROS_AUTH_FAIL_ERR("db-conn-000001", "Kerberos 认证失败"),
    KERBEROS_AUTH_SUCCESS_GET_CONN_FAIL_ERR("db-conn-000002", "Kerberos 认证成功 但获取连接失败"),
    HIVE_CONN_USER_PWD_ERR("db-conn-000003", "Hive连接失败 账号或密码错误"),
    HIVE_CONN_ERR("db-conn-000004", "Hive连接失败 请检查连接"),
    HIVE_DRIVE_LOAD_ERR("db-conn-000005", "Hive驱动加载失败"),
    SQL_EXEC_ERR("db-conn-000005", "执行SQL错误"),



    SYSTEM_INNER_ERR("db-conn-100000", "系统内部错误"),
    ;

    /**键和值定义为code, value 实现BaseEnums+@Getter完成get方法*/
    private final String code;
    private final String value;

    ResultCode(String code, String value) {
        this.code = code;
        this.value = value;
    }
}
