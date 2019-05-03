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
import java.util.LinkedList;
import java.util.List;

/**
 * Created by pengan on 17-3-3.
 */
public class CreateZNodesHandler extends AbstractHandler {
    private static final Logger logger = Logger.getLogger(CreateZNodesHandler.class);
    private static final String CREATE_ZNODES_ROUTE = "/create/znodes";

    public static void register() {
        ApiCenter.register(CREATE_ZNODES_ROUTE, new CreateZNodesHandler());
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

        try {
            if (metaData.getSlave() != null && metaData.getSlave().getBinlogPos() == 0) {
                throw new Exception("binlog position == 0 error default value should be set to 4!!");
            }

            String host = metaData.getDbInfo().getHost();
            int port = metaData.getDbInfo().getPort();

            try {
                // 校验实例是否已存在
                if (service.hostExist(host, port + "")) {
                    //实例已存在则进行实例的元数据更新
                    try {
                        service.setDbInfo(metaData.getDbInfo());
                    } catch (Exception e) {
                        logger.error(e);
                        ApiCenter.write(request, httpServletResponse,
                                ApiCenter.format(Constants.UPDATE_INSTANCE_FAILURE, e.getMessage()),
                                HttpServletResponse.SC_OK);
                        return;
                    }
                    ApiCenter.write(request, httpServletResponse,
                            ApiCenter.format(Constants.SUCCESS, Constants.EMPTY),
                            HttpServletResponse.SC_OK);
                    return;
                }
            } catch (Exception e) {
                logger.error(e);
                ApiCenter.write(request, httpServletResponse,
                        ApiCenter.format(Constants.ZK_QUERY_FAILURE, e.getMessage()),
                        HttpServletResponse.SC_OK);
                return;
            }

            // 如果不存在节点 则创建
            List<Meta.MetaData> metaInfoList = new LinkedList<Meta.MetaData>();
            metaInfoList.add(metaData);
            service.batchCreate(metaInfoList);

            ApiCenter.write(request, httpServletResponse,
                    ApiCenter.format(Constants.SUCCESS, Constants.EMPTY),
                    HttpServletResponse.SC_OK);
        } catch (Exception e) {
            logger.error("create znode error: " + ExceptionUtils.getStackTrace(e));
            ApiCenter.write(request, httpServletResponse,
                    ApiCenter.format(Constants.ZK_NODE_CREATE_FAILURE, e.getMessage()),
                    HttpServletResponse.SC_OK);
        } finally {
            service.close();
        }
    }
}
