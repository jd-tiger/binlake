package com.jd.binlog.dbsync.event;


import com.jd.binlog.dbsync.LogBuffer;
import com.jd.binlog.dbsync.LogEvent;
import com.jd.binlog.util.UUIDUtils;

/**
 * @author jianghang 2013-4-8 上午12:36:29
 * @version 1.0.3
 * @since mysql 5.6 / mariadb10
 */
public class GtidLogEvent extends LogEvent {

    // / Length of the commit_flag in event encoding
    public static final int ENCODED_FLAG_LENGTH = 1;
    // / Length of SID in event encoding
    public static final int ENCODED_SID_LENGTH  = 16;

    private boolean         commitFlag;
    private String sid;     // server uuid
    private long gtidGNO;   // transaction NO.
    private long crc;       // crc32 for 4 bytes

    public GtidLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        // final int postHeaderLen = descriptionEvent.postHeaderLen[header.type
        // - 1];

        buffer.position(commonHeaderLen);
        commitFlag = (buffer.getUint8() != 0); // ENCODED_FLAG_LENGTH

        // ignore gtid info read
        // sid.copy_from((uchar *)ptr_buffer);
        // ptr_buffer+= ENCODED_SID_LENGTH;
        //
        // // SIDNO is only generated if needed, in get_sidno().
        // spec.gtid.sidno= -1;
        //
        // spec.gtid.gno= uint8korr(ptr_buffer);
        // ptr_buffer+= ENCODED_GNO_LENGTH;

        sid = UUIDUtils.hexBytesToUUID(buffer.getData(ENCODED_SID_LENGTH));
        gtidGNO = buffer.getLong64();

        // if it has remaining then crc32 on the tail take off 4bytes
        if (buffer.hasRemaining() && buffer.remaining() == 4) {
            crc = buffer.getUint32();
        }
    }

    public boolean isCommitFlag() {
        return commitFlag;
    }

    public String getSid() {
        return sid;
    }

    public long getGtidGNO() {
        return gtidGNO;
    }
}
