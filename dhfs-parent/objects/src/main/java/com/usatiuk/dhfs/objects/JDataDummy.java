package com.usatiuk.dhfs.objects;

public class JDataDummy implements JData {
    public static final JObjectKey TX_ID_OBJ_NAME = JObjectKey.of("tx_id");
    private static final JDataDummy INSTANCE = new JDataDummy();

    public static JDataDummy getInstance() {
        return INSTANCE;
    }

    @Override
    public JObjectKey key() {
        return TX_ID_OBJ_NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return true;
    }

    // hashCode
    @Override
    public int hashCode() {
        return 0;
    }
}
