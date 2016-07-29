package com.githoo.tool.redismigration.redis;

/**
 * Created with IntelliJ IDEA.
 * User: githoo
 * Date: 14-10-17
 * Time: 下午2:34
 * To change this template use File | Settings | File Templates.
 */
public class RedisKeyValue {
    private byte[] key;
    private byte[] value;
    private boolean isEmpety;

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public boolean isEmpety() {
        return isEmpety;
    }

    public void setEmpety(boolean empety) {
        isEmpety = empety;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
