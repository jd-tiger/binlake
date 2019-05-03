package com.jd.binlake.tower.api;

import com.google.protobuf.InvalidProtocolBufferException;
import com.jd.binlake.tower.http.HttpUtils;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.util.HexUtils;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by pengan on 17-3-2.
 */
public class ApiCenter {
    /**
     * createZNodes: {path: create/znodes}
     * <p>
     */
    final static String HTTP_CONTENT_TYPE = "text/html;charset=utf-8";
    public static final Set<ContextHandler> CONTEXTS = new LinkedHashSet<ContextHandler>();

    static void register(String route, AbstractHandler handler) {
        ContextHandler context = new ContextHandler();
        context.setContextPath(route);
        context.setResourceBase(".");
        context.setClassLoader(Thread.currentThread().getContextClassLoader());
        context.setHandler(handler);
        CONTEXTS.add(context);
    }

    static void write(HttpServletRequest request, HttpServletResponse response,
                      String result, int httpCode) throws IOException {
        response.setContentType(HTTP_CONTENT_TYPE);
        response.setStatus(httpCode);
        ((Request) request).setHandled(true);
        response.getWriter().println(result);
    }

    static String format(int code, String errMsg) {
        return HttpUtils.format(code, errMsg);
    }

    static Meta.MetaData getPostMetaData(Request request, HttpServletResponse httpServletResponse) throws IOException {
        StringBuilder bufferString = new StringBuilder();
        String line = null;

        while ((line = request.getReader().readLine()) != null) {
            bufferString.append(line);
        }

        byte[] bytes = HexUtils.decodeString(bufferString.toString());
        Meta.MetaData metaData = null;
        try {
            metaData = Meta.MetaData.unmarshalJson(bytes);
        } catch (Exception exception) {
            ApiCenter.write(request, httpServletResponse,
                    ApiCenter.format(Constants.JSON_PARSE_ERROR, exception.getLocalizedMessage()),
                    HttpServletResponse.SC_OK);
            return null;
        }
        return metaData;
    }

    public static String makeZNodePath(String host, String port) {
        return host + Constants.ZNODE_SEPARATOR + port;
    }
}
