package com.usatiuk.dhfs.syncmap;

import com.usatiuk.dhfs.remoteobj.JDataRemote;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;

public interface DtoMapper<F extends JDataRemote, D extends JDataRemoteDto> {
    D toDto(F obj);

    F fromDto(D dto);
}
