package com.usatiuk.objects.iterators;

import java.util.Optional;

public record DataWrapper<V>(V value) implements Data<V> {
}
