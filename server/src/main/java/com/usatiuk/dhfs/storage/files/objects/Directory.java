package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

@Accessors(chain = true)
@Getter
@Setter
public class Directory extends DirEntry {
    Collection<Pair<String, UUID>> children = new ArrayList<>();
}
