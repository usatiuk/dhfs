package com.usatiuk.dhfs.objects.repository.syncmap;

import com.usatiuk.dhfs.objects.JDataRemote;
import com.usatiuk.dhfs.objects.repository.JDataRemoteDto;

public interface DtoMapper<F extends JDataRemote, D extends JDataRemoteDto> {
    D toDto(F obj);

    F fromDto(D dto);
}
