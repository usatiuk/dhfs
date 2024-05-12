package com.usatiuk.dhfs.storage.data;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
@Getter
@Setter
public class Namespace {
    String name;
}
