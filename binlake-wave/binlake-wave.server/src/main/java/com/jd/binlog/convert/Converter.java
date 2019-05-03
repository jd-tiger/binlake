package com.jd.binlog.convert;

import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.exception.ErrorCode;
import com.jd.binlog.inter.msg.IConvert;
import com.jd.binlog.util.LogUtils;

import java.lang.reflect.Constructor;

/**
 * Created on 18-7-16
 *
 * @author pengan
 */
public class Converter {
    /**
     * 根据类名 反射生成 转换器 避免每次生成都需要修改代码 只需要自己实现就 ok
     *
     * @param name
     * @return
     */
    public static IConvert initConverter(String name) {
        LogUtils.info.info("convert class name " + name);
        Class cl = null;
        try {
            cl = Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new BinlogException(ErrorCode.ERR_CONVERTER_CLASS_NOT_FOUND, e);
        }

        Constructor cons = null;
        try {
            cons = cl.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new BinlogException(ErrorCode.ERR_CONVERTER_CONSTRUCTOR,
                    "should implement interface IConvert");
        }

        try {
            return (IConvert) cons.newInstance();
        } catch (Exception e) {
            throw new BinlogException(ErrorCode.ERR_CONVERTER_INIT, e);
        }
    }


    public static void main(String[] args) {
        IConvert conv = initConverter("com.jd.binlog.convert.ProtobufConverter");
        System.err.println("conv " + conv.getClass());
    }
}
