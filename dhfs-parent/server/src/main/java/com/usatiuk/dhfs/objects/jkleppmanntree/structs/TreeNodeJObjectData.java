package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.OnlyLocal;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import com.usatiuk.kleppmanntree.TreeNode;
import lombok.Getter;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

// FIXME: Ideally this is two classes?
@OnlyLocal
public class TreeNodeJObjectData extends JObjectData {
    @Getter
    final TreeNode<Long, UUID, JTreeNodeMeta, String> _node;

    public TreeNodeJObjectData(TreeNode<Long, UUID, JTreeNodeMeta, String> node) {
        _node = node;
    }

    @Override
    public String getName() {
        return _node.getId();
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return null;
    }

    @Override
    public Collection<String> extractRefs() {
        if (_node.getMeta() instanceof JTreeNodeMetaFile)
            return List.of(((JTreeNodeMetaFile) _node.getMeta()).getFileIno());
        return Collections.unmodifiableCollection(_node.getChildren().values());
    }

    @Override
    public Class<? extends JObjectData> getRefType() {
        return JObjectData.class;
    }
}
