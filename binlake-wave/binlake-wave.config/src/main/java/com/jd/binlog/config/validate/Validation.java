package com.jd.binlog.config.validate;

/**
 * Created by pengan on 17-2-21.
 *
 * each Configuration has to validate it self
 */
public interface Validation {
    void validate() throws IllegalArgumentException;
}
