package io.basestar.storage.sql.mapping;

import com.google.common.collect.ImmutableSet;
import io.basestar.util.Name;

import java.util.Set;

public class LinkMapping {

    public Set<Name> supportedExpand(final Set<Name> branch) {

        return ImmutableSet.of(Name.empty());
    }
}
