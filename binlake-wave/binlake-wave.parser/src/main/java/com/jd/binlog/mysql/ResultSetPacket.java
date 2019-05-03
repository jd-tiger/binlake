package com.jd.binlog.mysql;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

public class ResultSetPacket {

    private SocketAddress     sourceAddress;
    private List<FieldPacket> fields = new ArrayList<FieldPacket>();
    private List<String>      fieldValues      = new ArrayList<String>();

    public void setFields(List<FieldPacket> fields) {
        this.fields = fields;
    }

    public List<FieldPacket> getFields() {
        return fields;
    }

    public void setFieldValues(List<String> fieldValues) {
        this.fieldValues = fieldValues;
    }

    public List<String> getFieldValues() {
        return fieldValues;
    }

    public void setSourceAddress(SocketAddress sourceAddress) {
        this.sourceAddress = sourceAddress;
    }

    public SocketAddress getSourceAddress() {
        return sourceAddress;
    }

    public String toString() {
        return "ResultSetPacket [fields=" + fields + ", fieldValues=" + fieldValues
               + ", sourceAddress=" + sourceAddress + "]";
    }

}
