package com.jd.binlog.client;

import java.util.List;

/**
 * Created by pengan on 17-3-8.
 */
public interface MessageDeserialize<T> {
    List<EntryMessage> deserialize(List<T> msgs) throws Exception;
}
