package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.objects.JData;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public interface OpExtractor<T extends JData> {
    Pair<List<Op>, Runnable> extractOps(T data, PeerId peerId);
}
