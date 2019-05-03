package com.jd.binlog.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created on 18-7-9
 *
 * @author pengan
 */
public class GTIDUtils {
    static final int RANGE_EQUAL = 0x01; // 区间相等
    static final int RANGE_CONTAINING = 0x02; // 区间包含 左 包含 右
    static final int RANGE_CONTAINED = 0x04; // 区间被包含 左 包含于 右
    static final int RANGE_INTERSECT = 0x08; // 区间有交集
    static final int RANGE_NO_INTERSECT = 0x11; // 区间无交集


    /**
     * 要求:
     * 如果gtid-set 转换成map
     * 默认 如果长度较长 的gtid集合 不包含较短的gtid集合 则抛出异常
     *
     * @param src
     * @param dst
     * @return <p>
     * 0: 表示 相等
     * </p>
     * <p>
     * 1: 表示 src 包含 target
     * </p>
     * <p>
     * <p>
     * -1: 表示 src 包含于 target
     * </p>
     * @throws Exception
     */
    public static int compare(String src, String dst) throws Exception {
        int rs1 = compare0(src, dst) ^ RANGE_EQUAL; // 去除 相等的集合 不影响结果
        int rs2 = compare0(dst, src) ^ RANGE_EQUAL; // 去除 相等的集合 不影响结果

        if (rs1 == rs2) {
            // gtid 区间相等
            return 0;
        }

        if (rs1 == RANGE_CONTAINING && rs2 == RANGE_CONTAINED) {
            // 包含
            return 1;
        }

        if (rs2 == RANGE_CONTAINING && rs1 == RANGE_CONTAINED) {
            // 被包含
            return -1;
        }

        if (rs1 == RANGE_INTERSECT || rs2 == RANGE_INTERSECT) {
            throw new Exception("gtid 之间存在有交集无法判断两者大小");
        }

        if (rs1 == RANGE_NO_INTERSECT || rs2 == RANGE_NO_INTERSECT) {
            throw new Exception("gtid 不存在交集 无法判断两者大小");
        }

        throw new Exception("gtid 集合之间多种关系并存 无法判断两者大小 ");
    }

    /**
     * <p>
     * 按照范围大小来区分大小写
     * <b>比较相同的uuid</b>
     * <p>
     * 分5类: 区间有大, 小, 相等, 交叉, 无交叉
     * <p>
     * <p>
     * <p>
     * <p>样例</p>
     * 9583d493-ce62-11e6-91a1-507b9d578e91:1-4:6-9:12-15:18,
     * 9584d493-ce62-11e6-91a1-507b9d578e91:1-4:6-9:12-15:18
     *
     * @param src
     * @param dest
     * @return
     */
    private static int compare0(String src, String dest) throws Exception {
        Map<String, ArrayList<Long[]>> srcM = toMap(src);
        Map<String, ArrayList<Long[]>> destM = toMap(dest);

        int rst = 0x00;
        for (Map.Entry<String, ArrayList<Long[]>> entry : srcM.entrySet()) {
            String uid = entry.getKey();
            ArrayList<Long[]> srcRgs = entry.getValue();

            ArrayList<Long[]> targetRgs = null;
            if ((targetRgs = destM.get(uid)) != null) {
                // 某一个uuid 下 range 是单调递增

                for (int i = 0; i < srcRgs.size(); i++) {
                    Long[] srcRg = srcRgs.get(i);
                    lp2:
                    for (int j = 0; j < targetRgs.size(); j++) {
                        Long[] targetRg = targetRgs.get(j);
                        switch (rangeRelation(srcRg, targetRg)) {
                            case RANGE_EQUAL: // 相等
                                rst |= RANGE_EQUAL;
                                break lp2; // 已经找到区间 则可以直接退出2循环
                            case RANGE_CONTAINING: // 包含 继续拿同一区间往下排查
                                rst |= RANGE_CONTAINING;
                                break;
                            case RANGE_CONTAINED: // 包含于 因为只可能包含在一部分 所以直接退出lp2循环
                                rst |= RANGE_CONTAINED;
                                break lp2;
                            case RANGE_INTERSECT: // 相交 相交的话 退出循环 lp2
                                rst |= RANGE_INTERSECT;
                                break lp2;
                            case RANGE_NO_INTERSECT: // 不相交 继续往下走
                                if (j == targetRgs.size() - 1) { // 到最后一个
                                    // 源gtid-sets 存在 目标 gtid-sets不存在的 区间
                                    rst |= RANGE_NO_INTERSECT;
                                }
                                break;
                        }
                    }
                }
            }
        }


        if (srcM.size() > destM.size()) {
            if (rst != RANGE_CONTAINING) { // src 不包含 dest gtid
                throw new Exception("由于 " + src + " gtid-set 长度 > " + dest + " gtid-set 但是前者gtid集合不包含后者");
            }
        }

        if (srcM.size() < destM.size()) {
            if (rst != RANGE_CONTAINED) { // src 不包含于 dest gtid
                throw new Exception("由于 " + src + " gtid-set 长度 < " + dest + " gtid-set, 但是前者gtid集合不包含于后者");
            }
        }

        return rst;
    }

    /**
     * range relation:
     * <p>
     * 00000001: 相等
     * 00000010: src 包含 dest
     * 00000100: src 包含于 dest
     * 00001000: src 与 dest 相交
     * 00010000: src 与 dest 不相交
     *
     * @param src
     * @param dest
     * @return
     */
    private static int rangeRelation(Long[] src, Long[] dest) {
        long srcBegin = src[0];
        long srcEnd = src[1];
        long tarBegin = dest[0];
        long tarEnd = dest[1];

        // 两区间完全相等
        if (srcBegin == tarBegin && srcEnd == tarEnd) {
            // 相等则继续判断
            return RANGE_EQUAL;
        }

        // 区间大 源范围大
        if (srcBegin <= tarBegin && srcEnd >= tarEnd) {
            // source 范围要大
            return RANGE_CONTAINING;
        }

        // 区间小 目标范围大
        if (tarBegin <= srcBegin && tarEnd >= srcEnd) {
            // 目标 范围要大
            return RANGE_CONTAINED;
        }

        if (srcEnd < tarBegin || srcBegin > tarEnd) {
            // 不相交
            return RANGE_NO_INTERSECT;
        }

        return RANGE_INTERSECT;
    }

    private static Map<String, ArrayList<Long[]>> toMap(String gidSets) {
        // gtid maps
        Map<String, ArrayList<Long[]>> idms = new HashMap<String, ArrayList<Long[]>>();

        for (String gid : gidSets.split(",")) {
            String[] ids = gid.split(":");
            String uuid = ids[0].trim(); // prevent to use the gtid with blank
            ArrayList<Long[]> ranges = null;
            if ((ranges = idms.get(uuid)) == null) {
                ranges = new ArrayList<>();
                idms.put(uuid, ranges);
            }

            for (int i = 1; i < ids.length; i++) {
                String[] rgs = ids[i].split("-");
                long start = Long.parseLong(rgs[0]);
                long end = start;
                if (rgs.length == 2) {
                    end = Long.parseLong(ids[i].split("-")[1]);
                }
                ranges.add(new Long[]{start, end});
            }
        }
        return idms;
    }

    public static void main(String[] args) throws Exception {
        String gtid1 = "bd4e26ed-2bd7-4a13-9470-5d6d6f0bebb0:1-50,5ecf7dc9-cfbc-47d3-89ed-f9bacde3e0b4:3-50";
        String gtid2 = "bd4e26ed-2bd7-4a13-9470-5d6d6f0bebb0:1:6:12-50,5ecf7dc9-cfbc-47d3-89ed-f9bacde3e0b4:3-50";
        System.err.println("gtid1 " + compare(gtid1, gtid2) + " gtid2");
    }
}
