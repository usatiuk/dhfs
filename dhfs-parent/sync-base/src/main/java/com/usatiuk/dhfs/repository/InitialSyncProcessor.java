package com.usatiuk.dhfs.repository;

import com.usatiuk.dhfs.PeerId;
import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;

public interface InitialSyncProcessor<T extends JData> {
    void prepareForInitialSync(PeerId from, JObjectKey key);
    void handleCrash(JObjectKey key);
}
