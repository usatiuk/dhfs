package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.objects.JDataRemote;
import com.usatiuk.dhfs.objects.persistence.JDataRemoteDtoP;

import java.io.Serializable;

@ProtoMirror(JDataRemoteDtoP.class)
public interface JDataRemoteDto extends Serializable {
    default Class<? extends JDataRemote> objClass() {
        assert JDataRemote.class.isAssignableFrom(getClass());
        return (Class<? extends JDataRemote>) this.getClass();
    }
}
