package com.usatiuk.dhfs.objects;

import jakarta.annotation.Nonnull;
import lombok.Builder;

import java.io.Serializable;

@Builder
public record JDataVersionedWrapper<T extends JData>(@Nonnull T data, long version) implements Serializable {
}
