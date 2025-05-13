package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.objects.JData;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * Interface for extracting operations from data objects.
 * @param <T> the type of data
 */
public interface OpExtractor<T extends JData> {
    /**
     * Extract operations from the given data object.
     *
     * @param data  the data object to extract operations from
     * @param peerId the ID of the peer to extract operations for
     * @return a pair of a list of operations and a runnable to execute after the operations are sent to the peer
     */
    Pair<List<Op>, Runnable> extractOps(T data, PeerId peerId);
}
