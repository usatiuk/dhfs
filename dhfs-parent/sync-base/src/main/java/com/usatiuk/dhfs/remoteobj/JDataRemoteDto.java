package com.usatiuk.dhfs.remoteobj;

import java.io.Serializable;

public interface JDataRemoteDto extends Serializable {
    default Class<? extends JDataRemote> objClass() {
        assert JDataRemote.class.isAssignableFrom(getClass());
        return (Class<? extends JDataRemote>) this.getClass();
    }
}
