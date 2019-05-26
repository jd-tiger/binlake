package com.jd.binlog.meta;

import com.jd.binlog.util.ConstUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.*;

/**
 * Any Meta Utils including convert, make path, and assemble db-info, rule, mq rule, filter, etc.
 *
 * @author pengan
 */
public class MetaUtils {
    private static final Logger logger = Logger.getLogger(MetaUtils.class);

    /**
     * filter prefix including white, black
     */
    private static final String FilterWhitePrefix = "white";
    private static final String FilterBlackPrefix = "black";

    /**
     * ephemeral sequence node
     */
    private static final String LOCK_NAME = "lock-";

    /**
     * zk path
     */

    private static Set<String> fixPaths = new HashSet<>();

    static {
        fixPaths.add(ConstUtils.ZK_COUNTER_PATH.substring(1));
        fixPaths.add(ConstUtils.ZK_DYNAMIC_PATH.substring(1));
        fixPaths.add(ConstUtils.ZK_TERMINAL_PATH.substring(1));
        fixPaths.add(ConstUtils.ZK_CANDIDATE_PATH.substring(1));
        fixPaths.add(ConstUtils.ZK_LEADER_PATH.substring(1));
        fixPaths.add(ConstUtils.ZK_ALARM_PATH.substring(1));
        fixPaths.add(ConstUtils.ZK_ERROR_PATH.substring(1));
    }

    /**
     * @param client: curator client as zk client
     * @param dbPath: MySQL instance path => data path for db info
     * @return
     */
    public static Meta.DbInfo dbInfo(CuratorFramework client, String dbPath) throws Exception {
        dbPath = dbPath.endsWith("/") ? dbPath.substring(0, dbPath.length() - 1) : dbPath;

        Meta.DbInfo dbInfo = Meta.DbInfo.unmarshalJson(client.getData().forPath(dbPath));

        // rules
        List<Meta.Rule> rls = new LinkedList<>();

        // children for child path
        for (String c : client.getChildren().forPath(dbPath)) {
            // children for path
            if (fixPaths.contains(c) || c.contains(LOCK_NAME)) {
                continue;
            }

            // rule path
            String rp = dbPath + File.separator + c;
            rls.add(getRule(client, rp));
        }

        // add rule info
        dbInfo.setRule(rls);

        return dbInfo;
    }

    /**
     * @param c:        curator client
     * @param rulePath: rule path
     * @return
     * @throws Exception
     */
    private static Meta.Rule getRule(CuratorFramework c, String rulePath) throws Exception {
        logger.debug("get rule ");
        Meta.Rule r = Meta.Rule.unmarshalJson(c.getData().forPath(rulePath));

        switch (r.getStorage()) {
            case MQ_STORAGE:
                for (String p : c.getChildren().forPath(rulePath)) {
                    // children for path
                    if (fixPaths.contains(p) || p.contains(LOCK_NAME)) {
                        continue;
                    }

                    // mq rule path
                    String mrp = rulePath + File.separator + p;
                    if (logger.isDebugEnabled()) {
                        logger.debug("mq rule path " + mrp);
                    }
                    r.setAny(Meta.MQRule.marshalJson(getMQRule(c, mrp)));
                }

                break;
            case KV_STORAGE:
                break;
        }

        return r;
    }

    /**
     * @param c:   curator client
     * @param mrp: mq rule path
     * @return
     */
    private static Meta.MQRule getMQRule(CuratorFramework c, String mrp) throws Exception {
        logger.debug("get mq rule");
        Meta.MQRule mr = Meta.MQRule.unmarshalJson(c.getData().forPath(mrp));

        // white filters
        List<Meta.Filter> wfs = new LinkedList<>();

        // black filters
        List<Meta.Filter> bfs = new LinkedList<>();
        for (String p : c.getChildren().forPath(mrp)) {
            // get path for rule info
            String fp = mrp + File.separator + p;
            if (logger.isDebugEnabled()) {
                logger.debug("filter path " + fp);
            }
            Meta.Filter f = getFilter(c, fp);
            if (fp.startsWith(FilterWhitePrefix)) {
                wfs.add(f);
            }

            if (fp.startsWith(FilterBlackPrefix)) {
                bfs.add(f);
            }
        }

        // set white filters
        mr.setWhite(wfs);

        // set black filters
        mr.setBlack(bfs);
        return mr;
    }

    /**
     * @param c:  curator client
     * @param fp: filter path
     * @return
     */
    private static Meta.Filter getFilter(CuratorFramework c, String fp) {
        return null;
    }

    /**
     * @param zkPath: zk parent path
     * @param dbInfo: db info to bytes
     * @return
     * @throws Exception
     */
    public static Map<String, byte[]> dbBytesMap(String zkPath, Meta.DbInfo dbInfo) throws Exception {
        logger.debug("path bytes ");
        List<Meta.Rule> rls = dbInfo.getRule();
        dbInfo.setRule(null); // set to null to prevent too big for zk node value

        // path value bytes map
        Map<String, byte[]> pbs = new LinkedHashMap<>(); // must in order

        String dbPath = zkPath.endsWith("/") ? zkPath : (zkPath + "/") + nodePath(dbInfo.getHost(), dbInfo.getPort() + "");
        pbs.put(dbPath, Meta.DbInfo.marshalJson(dbInfo));

        if (rls != null) {
            // rules
            for (Meta.Rule r : rls) {
                ruleBytesMap(dbPath, r, pbs);
            }
        }

        return pbs;
    }

    /**
     * @param dbPath: db info path
     * @param r       : rule
     * @param pbs
     * @throws Exception
     */
    private static void ruleBytesMap(String dbPath, Meta.Rule r, Map<String, byte[]> pbs) throws Exception {
        logger.debug("rule into bytes ");
        String rulePath = dbPath + File.separator;

        byte[] any = r.getAny();

        switch (r.getStorage()) {
            case KV_STORAGE:
                break;
            case MQ_STORAGE:
                Meta.MQRule mr = Meta.MQRule.unmarshalJson(any);
                rulePath = rulePath + nodePath(r.getStorage().name(), mr.getTopic());

                // set rule path value
                r.setAny(null);
                pbs.put(rulePath, Meta.Rule.marshalJson(r));

                mqRuleBytesMap(rulePath, mr, pbs);
                break;
        }
    }

    /**
     * @param rulePath: rule path
     * @param mr        : mq rule
     * @param pbs       : path bytes maps
     */
    private static void mqRuleBytesMap(String rulePath, Meta.MQRule mr, Map<String, byte[]> pbs) throws Exception {
        logger.debug("mq rule bytes");
        String mrPath = rulePath + File.separator + mr.getTopic();

        // white filters
        List<Meta.Filter> wfs = mr.getWhite();
        List<Meta.Filter> bfs = mr.getBlack();

        // set null
        mr.setBlack(null);
        mr.setWhite(null);

        // set bytes value
        pbs.put(mrPath, Meta.MQRule.marshalJson(mr));

        if (wfs != null) {
            // white filter
            for (Meta.Filter f : wfs) {
                // filter path
                String fp = mrPath + File.separator + nodePath(FilterWhitePrefix, f.getTable());
                filterBytesMap(fp, f, pbs);
            }
        }

        if (bfs != null) {
            // black filter
            for (Meta.Filter f : bfs) {
                // filter path
                String fp = mrPath + File.separator + nodePath(FilterBlackPrefix, f.getTable());
                filterBytesMap(fp, f, pbs);
            }
        }
    }

    /**
     * @param fp: filter path
     * @param f:  filter
     */
    private static void filterBytesMap(String fp, Meta.Filter f, Map<String, byte[]> pbs) throws Exception {
        logger.debug("filter into bytes ");
        pbs.put(fp, Meta.Filter.marshalJson(f));
    }

    private static String nodePath(String pre, String suf) {
        return pre + ":" + suf;
    }
}
