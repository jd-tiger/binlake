package com.jd.binlog.domain;

import com.jd.binlog.config.bean.HttpConfig;
import com.jd.binlog.conn.MySQLConnector;
import com.jd.binlog.conn.MySQLExecutor;
import com.jd.binlog.meta.MetaInfo;
import com.jd.binlog.mysql.GTIDs;
import com.jd.binlog.mysql.ResultSetPacket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by pengan on 17-2-23.
 */
public class GTIDTracker extends DomainSwitchTracker {
    public GTIDTracker(MetaInfo metaInfo, HttpConfig httpConf) {
        super(metaInfo, httpConf);
    }

    public void trackEvent() throws IOException {
        trackGTID();
    }

    public void trackTransaction() throws IOException {
        trackGTID();
    }

    public void trackGTID() throws IOException {
        MySQLConnector connector = new MySQLConnector(metaInfo.getDbInfo());

        try {
            String executedGtidSet = binlogInfo.getExecutedGtidSets();

            if (executedGtidSet == null || executedGtidSet.equals("")) {
                throw new IOException("ExecutedGtidSet in Zookeeper is empty");
            }
            connector.handshake();

            if (!checkGTIDStatus(connector)) {
                throw new IOException("gtid_mode or enforce_gtid_consistency in mysql is off");
            }

            if (!checkMasterGTIDSet(connector, executedGtidSet)) {
                throw new IOException("Master is not incloude Zookeeper's executedGtidSet");
            }
        } catch (Exception exp) {
            throw new IOException(exp);
        }
    }

    private boolean checkGTIDStatus(MySQLConnector connector) throws Exception {
        try {
            MySQLExecutor executor = new MySQLExecutor(connector);
            ResultSetPacket rst = null;
            rst = executor.query("show variables like 'gtid_mode';");

            if (rst.getFieldValues().size() == 0) {
                return false;
            }
            String gitdModeStatus = rst.getFieldValues().get(1);
            rst = executor.query("show variables like 'enforce_gtid_consistency';");

            if (rst.getFieldValues().size() == 0) {
                return false;
            }
            String consistencyStatus = rst.getFieldValues().get(1);

            if (!gitdModeStatus.equals("ON") || !consistencyStatus.equals("ON")) {
                return false;
            }
        } catch (Exception e) {
            throw e;
        }
        return true;
    }

    private boolean checkMasterGTIDSet(MySQLConnector connector, String executedGtidSet) throws Exception {
        boolean flag = true;

        try {
            MySQLExecutor executor = new MySQLExecutor(connector);
            ResultSetPacket rst = null;
            rst = executor.query("show master status;");
            String gtidSet = rst.getFieldValues().get(4);
            List<GTIDs> list = getMasterGTIDSet(gtidSet);

            if (!checkExcutedGTID(list, executedGtidSet)) {
                flag = false;
            }

        } catch (Exception e) {
            throw e;
        }
        return flag;
    }

    private List<GTIDs> getMasterGTIDSet(String gtidSet) {
        List<GTIDs> list = new ArrayList<GTIDs>();
        String[] masterGTIDSet = gtidSet.split(",");

        for (int i = 0; i < masterGTIDSet.length; i++) {
            String[] gtid = masterGTIDSet[i].split(":");

            for (int j = 1; j < gtid.length; j++) {
                String[] gno = gtid[j].split("-");
                long gnoL;
                long gnoR;

                if (gno.length > 1) {
                    gnoL = Long.parseLong(gno[0]);
                    gnoR = Long.parseLong(gno[1]);
                } else {
                    if (gno.length == 1) {
                        gnoL = Long.parseLong(gno[0]);
                        gnoR = gnoL;
                    } else {
                        continue;
                    }
                }
                GTIDs gtiDs = new GTIDs(gtid[0], gnoL, gnoR);
                list.add(gtiDs);
            }
        }
        return list;
    }

    private boolean checkExcutedGTID(List<GTIDs> list, String binlogGTIDs) throws Exception {
        String[] gtids = binlogGTIDs.split(",");

        for (int i = 0; i < gtids.length; i++) {
            String[] gtid = gtids[i].split(":");

            for (int j = 1; j < gtid.length; j++) {
                String[] gno = gtid[j].split("-");
                long gnoL;
                long gnoR;

                if (gno.length > 1) {
                    gnoL = Long.parseLong(gno[0]);
                    gnoR = Long.parseLong(gno[1]);
                } else {
                    if (gno.length == 1) {
                        gnoL = Long.parseLong(gno[0]);
                        gnoR = gnoL;
                    } else {
                        continue;
                    }
                }
                boolean flag = false;

                for (GTIDs tmp : list) {
                    if (tmp.equalSid(gtid[0]) && tmp.inGnoSets(gnoL, gnoR)) {
                        flag = true;
                        break;
                    }
                }

                if (!flag) {
                    return false;
                }
            }
        }
        return true;
    }
}
