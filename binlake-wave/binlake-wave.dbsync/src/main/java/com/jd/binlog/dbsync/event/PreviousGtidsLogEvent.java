package com.jd.binlog.dbsync.event;


import com.jd.binlog.dbsync.GTID;
import com.jd.binlog.dbsync.LogBuffer;
import com.jd.binlog.dbsync.LogEvent;
import com.jd.binlog.util.UUIDUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author jianghang 2013-4-8 上午12:36:29
 * @version 1.0.3
 * @since mysql 5.6
 */
public class PreviousGtidsLogEvent extends LogEvent {


    private Map<String, GTID> preGTIDs = new LinkedHashMap<String, GTID>();
    private long crc;       // crc32 for 4 bytes

    public PreviousGtidsLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent) {
        super(header);
        // do nothing , just for mysql gtid search function

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        final int GTIDLength = 16;
        // final int postHeaderLen = descriptionEvent.postHeaderLen[header.type
        // - 1];

        buffer.position(commonHeaderLen);

        long nSids = buffer.getLong64();
        for (long sidIndex = 0; sidIndex < nSids; sidIndex++) {
            String uuid = UUIDUtils.hexBytesToUUID(buffer.getData(GTIDLength));
            GTID gtid = new GTID(uuid);
            long intervals = buffer.getLong64();
            for (long interIndex = 0; interIndex < intervals; interIndex ++) {
                GTID.ClosedInterval inter = new GTID.ClosedInterval(buffer.getLong64(), buffer.getLong64() - 1);
                gtid.addInterval(inter);
            }
            preGTIDs.put(uuid, gtid);
        }

        System.out.println("previous gtids : " + preGTIDs.toString());

        // if it has remaining then crc32 on the tail take off 4bytes
        if (buffer.hasRemaining() && buffer.remaining() == 4) {
            crc = buffer.getUint32();
        }
    }

    public Map<String, GTID> getPreGTIDs() {
        return preGTIDs;
    }
}
