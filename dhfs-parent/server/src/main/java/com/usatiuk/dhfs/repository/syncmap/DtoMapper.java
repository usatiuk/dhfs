package com.usatiuk.dhfs.repository.syncmap;

import com.usatiuk.dhfs.JDataRemote;
import com.usatiuk.dhfs.repository.JDataRemoteDto;

public interface DtoMapper<F extends JDataRemote, D extends JDataRemoteDto> {
    D toDto(F obj);

    F fromDto(D dto);
}
