package io.basestar.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.basestar.schema.exception.MissingMemberException;
import io.basestar.schema.use.Use;
import io.basestar.util.Path;

import java.util.Map;
import java.util.Set;

public interface Member extends Named, Described {

//    void resolve(String name, Schema schema, Namespace namespace);

    Object expand(Object value, Expander expander, Set<Path> expand);

//    Optional<Object> collapse(Object value, Set<Path> expand);

    Set<Path> requireExpand(Set<Path> paths);

    Use<?> typeOf(Path path);

//    interface Config extends Described {
//
//    }

    interface Resolver {

        @JsonIgnore
        Map<String, ? extends Member> getDeclaredMembers();

        @JsonIgnore
        Map<String, ? extends Member> getAllMembers();

        Member getMember(String name, boolean inherited);

        default Member requireMember(final String name, final boolean inherited) {

            final Member result = getMember(name, inherited);
            if (result == null) {
                throw new MissingMemberException(name);
            } else {
                return result;
            }
        }
    }
}
