package com.usatiuk.objects.transaction;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;

public class TxRecord {
    public interface TxObjectRecord<T> {
        JObjectKey key();
    }

    public record TxObjectRecordWrite<T extends JData>(JData data) implements TxObjectRecord<T> {
        @Override
        public JObjectKey key() {
            return data.key();
        }
    }

    public record TxObjectRecordDeleted(JObjectKey key) implements TxObjectRecord<JData> {
    }
}
