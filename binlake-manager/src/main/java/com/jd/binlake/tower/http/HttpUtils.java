package com.jd.binlake.tower.http;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.server.Request;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.StringWriter;

/**
 * Created by pengan on 17-8-29.
 */
public class HttpUtils {
    public final static String HTTP_CONTENT_TYPE = "text/html;charset=utf-8";
    public final static String TOKEN = "token";
    public final static String CHARSET = "utf-8";


    public static void write(HttpServletRequest request, HttpServletResponse response,
                             String result, int httpCode) throws IOException {

        response.setContentType(HTTP_CONTENT_TYPE);
        response.setStatus(httpCode);
        ((Request) request).setHandled(true);
        response.getWriter().println(result);
    }

    public static String format(int code, String errMsg) {
        StringBuilder msg = new StringBuilder();
        ObjectMapper objectMap = new ObjectMapper();
        StringWriter sw = new StringWriter();

        JsonGenerator gen = null;
        try {
            gen = objectMap.getJsonFactory().createJsonGenerator(sw);
            gen.writeStartObject();

            gen.writeObjectField("code", code);
            gen.writeObjectField("message", errMsg);

            gen.writeEndObject();
            gen.flush();

            msg.append(sw.toString());

        } catch (IOException e) {
        } finally {
            if (gen != null) {
                try {
                    gen.close();
                } catch (IOException e) {
                }
            }
        }
        return msg.toString();
    }

}
