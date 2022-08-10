package com.itdl.properties;

import lombok.Data;
import lombok.EqualsAndHashCode;

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
