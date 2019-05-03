package com.google.common.collect;
/**
 * Created by pengan on 16-12-19.
 */

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.ConcurrentMap;

public class MigrateMap {

    @SuppressWarnings("deprecation")
    public static <K, V> ConcurrentMap<K, V> makeComputingMap(MapMaker maker,
                                                              Function<? super K, ? extends V> computingFunction) {
        return maker.makeComputingMap(computingFunction);
    }

    @SuppressWarnings("deprecation")
    public static <K, V> ConcurrentMap<K, V> makeComputingMap(Function<? super K, ? extends V> computingFunction) {
        return new MapMaker().makeComputingMap(computingFunction);
    }

    public static <K, V>LoadingCache<K, V> makeLoadingCache(CacheLoader<K, V> loader) {
        return CacheBuilder.newBuilder().build(loader);
    }
}
