package com.usatiuk.kleppmanntree;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TestLog implements LogInterface<Long, Long, TestNodeMeta, Long> {
    private final NavigableMap<CombinedTimestamp<Long, Long>, LogRecord<Long, Long, TestNodeMeta, Long>> _log = new TreeMap<>();

    @Override
    public Pair<CombinedTimestamp<Long, Long>, LogRecord<Long, Long, TestNodeMeta, Long>> peekOldest() {
        var ret = _log.firstEntry();
        if (ret == null) return null;
        return Pair.of(ret);
    }

    @Override
    public Pair<CombinedTimestamp<Long, Long>, LogRecord<Long, Long, TestNodeMeta, Long>> takeOldest() {
        var ret = _log.pollFirstEntry();
        if (ret == null) return null;
        return Pair.of(ret);
    }

    @Override
    public Pair<CombinedTimestamp<Long, Long>, LogRecord<Long, Long, TestNodeMeta, Long>> peekNewest() {
        var ret = _log.lastEntry();
        if (ret == null) return null;
        return Pair.of(ret);
    }

    @Override
    public List<Pair<CombinedTimestamp<Long, Long>, LogRecord<Long, Long, TestNodeMeta, Long>>> newestSlice(CombinedTimestamp<Long, Long> since, boolean inclusive) {
        var tail = _log.tailMap(since, inclusive);
        return tail.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();
    }

    @Override
    public List<Pair<CombinedTimestamp<Long, Long>, LogRecord<Long, Long, TestNodeMeta, Long>>> getAll() {
        return _log.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();
    }

    @Override
    public boolean isEmpty() {
        return _log.isEmpty();
    }

    @Override
    public boolean containsKey(CombinedTimestamp<Long, Long> timestamp) {
        return _log.containsKey(timestamp);
    }

    @Override
    public long size() {
        return _log.size();
    }

    @Override
    public void put(CombinedTimestamp<Long, Long> timestamp, LogRecord<Long, Long, TestNodeMeta, Long> record) {
        _log.put(timestamp, record);
    }

    @Override
    public void replace(CombinedTimestamp<Long, Long> timestamp, LogRecord<Long, Long, TestNodeMeta, Long> record) {
        _log.put(timestamp, record);
    }
}
