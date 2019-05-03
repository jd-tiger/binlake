package com.jd.binlog.http;

import java.util.LinkedList;
import java.util.List;

public class HttpServiceTest {
    public static void main(String[] args) {
        List<String> ips = new LinkedList<>();
        ips.add("127.0.0.1");
        ips.add("127.0.0.2");
        ips.add("127.0.0.3");
        ips.add("127.0.0.4");
        ips.add("127.0.0.5");
        ips.add("127.0.0.17");


        List<String> ips2 = new LinkedList<>();
        ips2.add("127.0.0.1");
        ips2.add("127.0.0.2");
//        ips2.add("127.0.0.3");
//        ips2.add("127.0.0.4");
//        ips2.add("127.0.0.5");
//        ips2.add("127.0.0.17");

        System.err.println(ips.containsAll(ips2));
    }
}
