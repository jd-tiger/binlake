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
 * Created by jingdi on 17-4-11.
 */
public class SetInstanceOffline extends AbstractHandler {
    private static final Logger logger = Logger.getLogger(SetInstanceOffline.class);

    private static final String SET_INSTANCESTATUS_OFFLINE = "/set/offline";

    public static void register() {
        ApiCenter.register(SET_INSTANCESTATUS_OFFLINE, new SetInstanceOffline());
    }

    @Override
    public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
        logger.debug("handle : " + s + ", request : " + request);
        Meta.MetaData metaData = ApiCenter.getPostMetaData(request, httpServletResponse);

        if (metaData == null) {
            return;
        }
        logger.info("request metadata : " + metaData);

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

            try {
                // no wait just return
                service.setOffline(host, port, false);
            } catch (Exception e) {
                logger.error(e.getMessage());
                ApiCenter.write(request, httpServletResponse,
                        ApiCenter.format(Constants.ZK_NODE_UPDATE_FAILURE, e.getLocalizedMessage()),
                        HttpServletResponse.SC_OK);
                return;
            }

            ApiCenter.write(request, httpServletResponse,
                    ApiCenter.format(Constants.SUCCESS, Constants.EMPTY),
                    HttpServletResponse.SC_OK);
        } catch (Exception e) {
            logger.error("set instance offline error: " + ExceptionUtils.getStackTrace(e));
            ApiCenter.write(request, httpServletResponse,
                    ApiCenter.format(Constants.ZK_QUERY_FAILURE, e.getMessage()),
                    HttpServletResponse.SC_OK);
        } finally {
            service.close();
        }
    }
}
