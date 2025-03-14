package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;

public interface PreCommitTxHook {
    default void onChange(JObjectKey key, JData old, JData cur) {
    }

    default void onCreate(JObjectKey key, JData cur) {
    }

    default void onDelete(JObjectKey key, JData cur) {
    }

    default int getPriority() {
        return 0;
    }
}
