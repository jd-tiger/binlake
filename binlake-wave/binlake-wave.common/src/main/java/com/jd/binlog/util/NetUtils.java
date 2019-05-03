package com.jd.binlog.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created on 18-4-17
 *
 * @author pengan
 */
public class NetUtils {
    /**
     * 获取本地 非loop 网卡ip
     *
     * @return
     * @throws Exception
     */
    public static String getHostAddress() throws Exception {
        String localAddr = null;
        Enumeration<NetworkInterface> inters = NetworkInterface.getNetworkInterfaces();
        InetAddress ip;
        while (inters.hasMoreElements()) {
            NetworkInterface ni = inters.nextElement();
            Enumeration<InetAddress> addrs = ni.getInetAddresses();
            while (addrs.hasMoreElements()) {
                ip = addrs.nextElement();
                if (!ip.isLoopbackAddress() && ip.getHostAddress().indexOf(':') == -1) {
                    // 多个网卡取字符串比较 最小值
                    if (localAddr == null) {
                        localAddr = ip.getHostAddress();
                        continue;
                    }
                    if (localAddr.compareTo(ip.getHostAddress()) > 0) {
                        localAddr = ip.getHostAddress();
                    }
                }
            }
        }

        if (localAddr == null) {
            throw new Exception("cannot find other network interface without loop ");
        }
        return localAddr;
    }

    /**
     * 判断是否为ipv4地址 *
     */
    public static boolean isIPv4Address(String ipv4Addr) {
        String lower = "(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])"; // 0-255的数字
        String regex = lower + "(\\." + lower + "){3}";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(ipv4Addr);
        return matcher.matches();
    }
}
