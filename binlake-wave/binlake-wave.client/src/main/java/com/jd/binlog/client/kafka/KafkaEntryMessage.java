package com.jd.binlog.client.kafka;

import com.jd.binlog.client.WaveEntry;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by ninet on 17-2-10.
 */
public class KafkaEntryMessage {

    private WaveEntry.Header header;
    private WaveEntry.EntryType entryType;
    private WaveEntry.TransactionBegin begin;
    private WaveEntry.TransactionEnd end;
    private WaveEntry.RowChange rowChange;
    private long batchId;
    private long inId;
    private String ip;
    private Map<String, Object> extensions;

    public KafkaEntryMessage(WaveEntry.Entry entry) {
        this.header = entry.getHeader();
        this.batchId = entry.getBatchId();
        this.inId = entry.getInId();
        this.ip = entry.getIp();
        this.extensions = new LinkedHashMap<String, Object>();
    }

    public WaveEntry.Header getHeader() {
        return header;
    }

    public void setHeader(WaveEntry.Header header) {
        this.header = header;
    }

    public WaveEntry.EntryType getEntryType() {
        return entryType;
    }

    public void setEntryType(WaveEntry.EntryType entryType) {
        this.entryType = entryType;
    }

    public WaveEntry.TransactionBegin getBegin() {
        return begin;
    }

    public void setBegin(WaveEntry.TransactionBegin begin) {
        this.begin = begin;
    }

    public WaveEntry.TransactionEnd getEnd() {
        return end;
    }

    public void setEnd(WaveEntry.TransactionEnd end) {
        this.end = end;
    }

    public WaveEntry.RowChange getRowChange() {
        return rowChange;
    }

    public void setRowChange(WaveEntry.RowChange rowChange) {
        this.rowChange = rowChange;
    }

    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    public long getInId() {
        return inId;
    }

    public void setInId(long inId) {
        this.inId = inId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Map<String, Object> getExtensions() {
        return extensions;
    }

    public void setExtensions(Map<String, Object> extensions) {
        this.extensions = extensions;
    }

    @Override
    public String toString() {
        return "EntryMessage{" +
                "header=" + header +
                ", entryType=" + entryType +
                ", begin=" + begin +
                ", end=" + end +
                ", rowChange=" + rowChange +
                ", batchId=" + batchId +
                ", inId=" + inId +
                ", ip='" + ip + '\'' +
                '}';
    }
}
