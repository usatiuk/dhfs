package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.Leaf;
import com.usatiuk.dhfs.objects.jrepository.OnlyLocal;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import com.usatiuk.kleppmanntree.CombinedTimestamp;
import com.usatiuk.kleppmanntree.LogRecord;
import lombok.Getter;

import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

// FIXME: Ideally this is two classes?
@OnlyLocal
@Leaf
public class JKleppmannTreeOpLog extends JObjectData {
    @Getter
    private final String _treeName;

    @Getter
    private final TreeMap<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>> _log;

    public JKleppmannTreeOpLog(String treeName, final TreeMap<CombinedTimestamp<Long, UUID>, LogRecord<Long, UUID, JKleppmannTreeNodeMeta, String>> log) {
        _treeName = treeName;
        _log = log;
    }

    public static String fromTreeName(final String treeName) {
        return treeName + "_oplog";
    }

    @Override
    public String getName() {
        return fromTreeName(_treeName);
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return null;
    }

    @Override
    public Collection<String> extractRefs() {
        return List.of();
    }
}
