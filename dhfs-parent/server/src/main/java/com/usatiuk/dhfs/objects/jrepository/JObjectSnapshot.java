package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;

public record JObjectSnapshot
        (ObjectMetadataP meta,
         JObjectDataP data,
         int externalHash) {
}
