package com.usatiuk.dhfs.repository;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.JDataRemote;
import com.usatiuk.dhfs.persistence.JDataRemoteDtoP;

import java.io.Serializable;

@ProtoMirror(JDataRemoteDtoP.class)
public interface JDataRemoteDto extends Serializable {
    default Class<? extends JDataRemote> objClass() {
        assert JDataRemote.class.isAssignableFrom(getClass());
        return (Class<? extends JDataRemote>) this.getClass();
    }
}
