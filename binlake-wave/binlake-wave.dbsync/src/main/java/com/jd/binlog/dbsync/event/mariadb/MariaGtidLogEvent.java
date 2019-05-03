package com.jd.binlog.dbsync.event.mariadb;


import com.jd.binlog.dbsync.LogBuffer;
import com.jd.binlog.dbsync.event.FormatDescriptionLogEvent;
import com.jd.binlog.dbsync.event.IgnorableLogEvent;
import com.jd.binlog.dbsync.event.LogHeader;

/**
 * mariadb的GTID_EVENT类型
 * 
 * @author jianghang 2014-1-20 下午4:49:10
 * @since 1.0.17
 */
public class MariaGtidLogEvent extends IgnorableLogEvent {

    public MariaGtidLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header, buffer, descriptionEvent);
        // do nothing , just ignore log event
    }

}
