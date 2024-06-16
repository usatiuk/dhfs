package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectChangelog;
import com.usatiuk.dhfs.objects.repository.distributed.ObjectChangelogEntry;
import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class ObjectMetaData implements Serializable {
    public ObjectMetaData(String name, Boolean assumeUnique) {
        _name = name;
        _assumeUnique = assumeUnique;
    }

    @Getter
    private final String _name;

    @Getter
    private final Boolean _assumeUnique;

    @Getter
    private final List<String> _remoteCopies = new ArrayList<>();

    @Getter
    private final HashMap<String, Long> _changelog = new LinkedHashMap<>();

    Long getTotalVersion() {
        return _changelog.values().stream().reduce(0L, Long::sum);
    }

    ObjectChangelog toRpcChangelog() {
        var changelogBuilder = ObjectChangelog.newBuilder();
        for (var m : getChangelog().entrySet()) {
            changelogBuilder.addEntries(ObjectChangelogEntry.newBuilder().setHost(m.getKey()).setVersion(m.getValue()).build());
        }
        return changelogBuilder.build();
    }

    ObjectHeader toRpcHeader() {
        var headerBuilder = ObjectHeader.newBuilder().setName(getName());
        headerBuilder.setAssumeUnique(getAssumeUnique());
        headerBuilder.setChangelog(toRpcChangelog());
        return headerBuilder.build();
    }
}
