package com.usatiuk.dhfs.repository;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;

public interface InitialSyncProcessor<T extends JData> {
    void prepareForInitialSync(PeerId from, JObjectKey key);
}
