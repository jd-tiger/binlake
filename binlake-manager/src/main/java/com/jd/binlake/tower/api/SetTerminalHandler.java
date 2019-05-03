package com.jd.binlake.tower.api;

import com.jd.binlake.tower.zk.ZkService;
import com.jd.binlog.meta.Meta;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * <p>
 * 设置终止节点 dump到某一个位置 即停止dump 数据
 */
public class SetTerminalHandler extends AbstractHandler {
    private static final Logger logger = Logger.getLogger(SetTerminalHandler.class);
    private static final String SET_TERMINAL = "/set/terminal";

    public static void register() {
        ApiCenter.register(SET_TERMINAL, new SetTerminalHandler());
    }

    @Override
    public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
        logger.info("handle: " + s);
        Meta.MetaData metaData = ApiCenter.getPostMetaData(request, httpServletResponse);

        if (metaData == null) {
            return;
        }
        logger.info(SET_TERMINAL + " request metadata : " + metaData);

        ZkService service = new ZkService(metaData.getZk().getServers(), metaData.getZk().getPath());
        try {
            String host = metaData.getDbInfo().getHost();
            int port = metaData.getDbInfo().getPort();
            //check host whether exist
            if (!service.hostExist(host, port + "")) {
                service.close();
                ApiCenter.write(request, httpServletResponse,
                        ApiCenter.format(Constants.INSTANCE_NOT_EXIST, "znode : " + host + ":" + port + " not exist"),
                        HttpServletResponse.SC_OK);
                return;
            }

            //触发选举
            service.setTerminal(host, port, metaData.getTerminal());

            ApiCenter.write(request, httpServletResponse,
                    ApiCenter.format(Constants.SUCCESS, Constants.EMPTY),
                    HttpServletResponse.SC_OK);
        } catch (Exception e) {
            logger.error("bind leader error: " + ExceptionUtils.getStackTrace(e));
            ApiCenter.write(request, httpServletResponse,
                    ApiCenter.format(Constants.BIND_LEADERS_FAILURE, e.getMessage()),
                    HttpServletResponse.SC_OK);
        } finally {
            service.close();
        }
    }

}
