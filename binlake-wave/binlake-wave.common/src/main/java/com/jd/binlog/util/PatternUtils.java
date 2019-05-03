package com.jd.binlog.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by xianmao.hexm 2011-5-5 下午09:34:11
 */
public class PatternUtils {


    private static LoadingCache<String, Pattern> patterns = CacheBuilder.newBuilder()
            .expireAfterWrite(30, TimeUnit.MINUTES)
            .refreshAfterWrite(30, TimeUnit.MINUTES)
            .build(new CacheLoader<String, Pattern>() {
                @Override
                public Pattern load(String pattern) throws Exception {
                    try {
                        PatternCompiler pc = new Perl5Compiler();
                        return pc.compile(
                                pattern,
                                Perl5Compiler.CASE_INSENSITIVE_MASK
                                        | Perl5Compiler.READ_ONLY_MASK
                                        | Perl5Compiler.SINGLELINE_MASK);
                    } catch (MalformedPatternException e) {
                        throw new NullPointerException("PatternUtils error!");
                    }
                }
            });
    public static Pattern getPattern(String pattern) {
        try {
            return patterns.get(pattern);
        } catch (ExecutionException e) {
            e.printStackTrace();
            return null;
        }
    }
}
