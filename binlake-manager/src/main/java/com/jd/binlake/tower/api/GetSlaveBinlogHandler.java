package com.jd.binlake.tower.api;

import com.jd.binlake.tower.zk.ZkService;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.util.HexUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Create by qianxi on 18-6-20
 */
public class GetSlaveBinlogHandler extends AbstractHandler {
    private static final Logger logger = Logger.getLogger(GetSlaveBinlogHandler.class);

    private static final String GET_SLAVE_BINLOG = "/slave/status";

    public static void register() {
        ApiCenter.register(GET_SLAVE_BINLOG, new GetSlaveBinlogHandler());
    }

    /**
     * request : MetaData
     *
     * @param s
     * @param request
     * @param httpServletRequest
     * @param httpServletResponse
     * @throws IOException
     * @throws ServletException
     */
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
                Meta.BinlogInfo slave = service.getBinlogInfo(ApiCenter.makeZNodePath(host, port + ""));
                metaData.setSlave(slave);
                logger.info("binlog info :\n" + slave);

                Meta.DbInfo dbInfo = service.getDbInfo(ApiCenter.makeZNodePath(host, port + ""));
                metaData.setDbInfo(dbInfo);
                logger.info("db info :\n" + dbInfo);
                logger.info("rules :");
                for (Meta.Rule rule : dbInfo.getRule()) {
                    logger.info(rule.getAny());
                }

                Meta.Counter counter = service.getCounter(ApiCenter.makeZNodePath(host, port + ""));
                metaData.setCounter(counter);
                logger.info("counter info :\n" + counter);

                Meta.Candidate cands = service.getCandidate(ApiCenter.makeZNodePath(host, port + ""));
                metaData.setCandidate(new LinkedList<String>(cands.getHost()));
                logger.info("candidates info :\n" + cands);

                Meta.Error error = service.getError(ApiCenter.makeZNodePath(host, port + ""));
                metaData.setError(error);
                logger.info("error info :\n" + error);
            } catch (Exception e) {
                logger.error(e.getMessage());
                ApiCenter.write(request, httpServletResponse,
                        ApiCenter.format(Constants.ZK_QUERY_FAILURE, e.getLocalizedMessage()),
                        HttpServletResponse.SC_OK);
                return;
            }

            String hex = HexUtils.encodeToString(Meta.MetaData.marshalJson(metaData));
            logger.info("hex value " + hex);
            ApiCenter.write(request, httpServletResponse,
                    hex,
                    HttpServletResponse.SC_OK);
        } catch (Exception e) {
            logger.error("query slave status error: " + ExceptionUtils.getStackTrace(e));
            ApiCenter.write(request, httpServletResponse,
                    ApiCenter.format(Constants.ZK_QUERY_FAILURE, e.getMessage()),
                    HttpServletResponse.SC_OK);
        } finally {
            service.close();
        }
    }
}
