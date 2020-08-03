package io.basestar.storage;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum Versioning {

    CHECKED(true),
    UNCHECKED(false);

    private final boolean checked;
}
