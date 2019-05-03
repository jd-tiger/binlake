package com.jd.binlog.dbsync.event.mariadb;


import com.jd.binlog.dbsync.LogBuffer;
import com.jd.binlog.dbsync.event.FormatDescriptionLogEvent;
import com.jd.binlog.dbsync.event.IgnorableLogEvent;
import com.jd.binlog.dbsync.event.LogHeader;

/**
 * mariadb的GTID_LIST_EVENT类型
 * 
 * @author jianghang 2014-1-20 下午4:51:50
 * @since 1.0.17
 */
public class MariaGtidListLogEvent extends IgnorableLogEvent {

    public MariaGtidListLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header, buffer, descriptionEvent);
        // do nothing , just ignore log event
    }

}
