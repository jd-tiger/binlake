package com.jd.binlog.dbsync.event.mariadb;


import com.jd.binlog.dbsync.LogBuffer;
import com.jd.binlog.dbsync.event.FormatDescriptionLogEvent;
import com.jd.binlog.dbsync.event.IgnorableLogEvent;
import com.jd.binlog.dbsync.event.LogHeader;

/**
 * mariadb10的BINLOG_CHECKPOINT_EVENT类型
 * 
 * @author jianghang 2014-1-20 下午2:22:04
 * @since 1.0.17
 */
public class BinlogCheckPointLogEvent extends IgnorableLogEvent {

    public BinlogCheckPointLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header, buffer, descriptionEvent);
        // do nothing , just mariadb binlog checkpoint
    }

}
