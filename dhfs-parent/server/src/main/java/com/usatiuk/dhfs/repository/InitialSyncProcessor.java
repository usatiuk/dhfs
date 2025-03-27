package com.usatiuk.dhfs.repository;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;

public interface InitialSyncProcessor<T extends JData> {
    void prepareForInitialSync(PeerId from, JObjectKey key);
}
