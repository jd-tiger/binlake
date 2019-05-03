package com.jd.binlake.tower.api;

import com.jd.binlake.tower.zk.ZkService;
import com.jd.binlog.meta.Meta;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class RemoveNodeHandler extends AbstractHandler {
    private static final Logger logger = Logger.getLogger(RemoveNodeHandler.class);
    private static final String REMOVE_ZNODE_ROUTE = "/remove/node";

    public static void register() {
        ApiCenter.register(REMOVE_ZNODE_ROUTE, new RemoveNodeHandler());
    }

    @Override
    public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
        logger.info("handle: " + s);
        Meta.MetaData metaData = ApiCenter.getPostMetaData(request, httpServletResponse);

        if (metaData == null) {
            return;
        }
        logger.info("request metadata : " + metaData);

        ZkService service = new ZkService(metaData.getZk().getServers(), metaData.getZk().getPath());

        String host = metaData.getDbInfo().getHost();
        int port = metaData.getDbInfo().getPort();
        try {
            // 校验实例是否已存在
            if (service.hostExist(host, port + "")) {
                //实例已存在则进行实例的元数据更新
                service.remove(host, port);
            }
            ApiCenter.write(request, httpServletResponse,
                    ApiCenter.format(Constants.SUCCESS, Constants.EMPTY),
                    HttpServletResponse.SC_OK);
        } catch (Exception e) {
            logger.error(e);
            ApiCenter.write(request, httpServletResponse,
                    ApiCenter.format(Constants.UPDATE_INSTANCE_FAILURE, e.getMessage()),
                    HttpServletResponse.SC_OK);
        } finally {
            service.close();
        }
    }
}
