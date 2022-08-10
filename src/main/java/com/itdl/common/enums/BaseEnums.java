package com.itdl.common.enums;

/**
 * @Description
 * @Author itdl
 * @Date 2022/08/08 10:45
 */
public interface BaseEnums<K, V> {

    /**
     * 获取枚举值的key
     */
    K getCode();

    /**
     * 获取枚举值的value
     */
    V getValue();

}
