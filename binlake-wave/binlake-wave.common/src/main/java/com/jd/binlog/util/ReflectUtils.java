package com.jd.binlog.util;

import java.lang.reflect.Field;

/**
 * Created by pengan on 16-12-27.
 */
public class ReflectUtils {
    /**
     * just support int long and string type value
     *
     * @param ob
     * @param field
     * @param value
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     */
    public static void setFieldValue(Object ob, String field, String value) throws IllegalAccessException, NoSuchFieldException {
        Field fd = ob.getClass().getDeclaredField(field);
        fd.setAccessible(true);
        if (fd.getType().getName().equals(java.lang.String.class.getName())) {
            fd.set(ob, value);
        } else if (fd.getType().getName().equals("int")) {
            fd.set(ob, Integer.parseInt(value));
        } else if (fd.getType().getName().equals("long")) {
            fd.set(ob, Long.parseLong(value));
        } else if (fd.getType().getName().equals("boolean")) {
            fd.set(ob, Boolean.parseBoolean(value));
        }
        fd.setAccessible(false);
    }
}
