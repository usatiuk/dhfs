package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.repository.JDataRemoteDto;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

public interface JDataRemote extends Serializable {
    JObjectKey key();

    default int estimateSize() {
        return 100;
    }

    default Collection<JObjectKey> collectRefsTo() {
        return List.of();
    }

    default Class<? extends JDataRemoteDto> dtoClass() {
        assert JDataRemoteDto.class.isAssignableFrom(getClass());
        return (Class<? extends JDataRemoteDto>) this.getClass();
    }
}
