package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

public class TxRecord {
    public interface TxObjectRecord<T> {
        JObjectKey key();
    }

    public record TxObjectRecordWrite<T extends JData>(JData data) implements TxObjectRecord<T> {
        @Override
        public JObjectKey key() {
            return data.getKey();
        }
    }

    public record TxObjectRecordDeleted(JObjectKey key) implements TxObjectRecord<JData> {
    }
}
