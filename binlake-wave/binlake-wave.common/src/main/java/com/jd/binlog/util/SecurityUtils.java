package com.jd.binlog.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by pengan on 16-9-25.
 */
public class SecurityUtils {

    public static final byte[] scramble411(byte[] pass, byte[] seed) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] pass1 = md.digest(pass);
        md.reset();
        byte[] pass2 = md.digest(pass1);
        md.reset();
        md.update(seed);
        byte[] pass3 = md.digest(pass2);
        for (int i = 0; i < pass3.length; i++) {
            pass3[i] = (byte) (pass3[i] ^ pass1[i]);
        }
        return pass3;
    }

    public static final String scramble323(String pass, String seed) {
        if ((pass == null) || (pass.length() == 0)) {
            return pass;
        }
        byte b;
        double d;
        long[] pw = hash(seed);
        long[] msg = hash(pass);
        long max = 0x3fffffffL;
        long seed1 = (pw[0] ^ msg[0]) % max;
        long seed2 = (pw[1] ^ msg[1]) % max;
        char[] chars = new char[seed.length()];
        for (int i = 0; i < seed.length(); i++) {
            seed1 = ((seed1 * 3) + seed2) % max;
            seed2 = (seed1 + seed2 + 33) % max;
            d = (double) seed1 / (double) max;
            b = (byte) Math.floor((d * 31) + 64);
            chars[i] = (char) b;
        }
        seed1 = ((seed1 * 3) + seed2) % max;
        seed2 = (seed1 + seed2 + 33) % max;
        d = (double) seed1 / (double) max;
        b = (byte) Math.floor(d * 31);
        for (int i = 0; i < seed.length(); i++) {
            chars[i] ^= (char) b;
        }
        return new String(chars);
    }

    private static long[] hash(String src) {
        long nr = 1345345333L;
        long add = 7;
        long nr2 = 0x12345671L;
        long tmp;
        for (int i = 0; i < src.length(); ++i) {
            switch (src.charAt(i)) {
                case ' ':
                case '\t':
                    continue;
                default:
                    tmp = (0xff & src.charAt(i));
                    nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
                    nr2 += ((nr2 << 8) ^ nr);
                    add += tmp;
            }
        }
        long[] result = new long[2];
        result[0] = nr & 0x7fffffffL;
        result[1] = nr2 & 0x7fffffffL;
        return result;
    }
    //将16进制的字符串转化为byte[]
    public static final byte[] hexToBytes(String hexPass){
        byte[] bytes;
        bytes = new byte[hexPass.length()/2];
        for(int i = 0; i < bytes.length; i++){
            bytes[i] = (byte) Integer.parseInt(hexPass.substring(i*2, i*2 + 2), 16);
        }
        return bytes;
    }

    //通过客户端发送的加密密码和数据库里hash密码进行比对转话是否一样
    //SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
    public static boolean checkPass(byte[] encryptPass, byte[] hexPassByte, byte[] seed) throws NoSuchAlgorithmException{
        byte[] token = new byte[40];
        byte[] calhexPass = new byte[20];
        System.arraycopy(seed, 0, token, 0, seed.length);
        System.arraycopy(hexPassByte, 0, token, seed.length, hexPassByte.length);
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] token_sha1 = md.digest(token);
        md.reset();
        for (int i = 0; i < token_sha1.length; i++) {
            calhexPass[i] = (byte) (token_sha1[i] ^ encryptPass[i]);
        }
        byte[] rs = md.digest(calhexPass);
        if (rs.length != hexPassByte.length)
            return false;

        for(int i = 0; i<rs.length; i++){
            if(rs[i]!=hexPassByte[i])
                return false;
        }
        return true;
    }
}
