package io.basestar.spark.expand;

import io.basestar.schema.Instance;
import io.basestar.schema.Link;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.util.Expander;
import io.basestar.util.Name;
import io.basestar.util.Page;
import lombok.Data;
import scala.Serializable;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public interface Expansion extends Serializable {

    LinkableSchema getTarget();

    Name getName();

    Set<Name> getExpand();

    static Set<Expansion> expansion(final LinkableSchema rootSchema, final Set<Name> expand) {

        return (new Expansion() {

            @Override
            public LinkableSchema getTarget() {

                return rootSchema;
            }

            @Override
            public Name getName() {

                return Name.of();
            }

            @Override
            public Set<Name> getExpand() {

                return expand;
            }
        }).next();
    }

    static Set<Expansion> expansion(final Set<Expansion> previous) {

        return previous.stream().flatMap(exp -> exp.next().stream()).collect(Collectors.toSet());
    }

    default Set<Expansion> next() {

        final LinkableSchema parentSchema = getTarget();
        final Name parentName = getName();
        final Set<Expansion> expansion = new HashSet<>();
        parentSchema.expand(new Expander.Noop() {
            @Override
            public Instance expandRef(final Name name, final ReferableSchema schema, final Instance ref, final Set<Name> expand) {

                expansion.add(new Expansion.OfRef(parentSchema, schema, parentName.with(name), expand));
                return super.expandRef(name, schema, ref, expand);
            }

            @Override
            public Page<Instance> expandLink(final Name name, final Link link, final Page<Instance> value, final Set<Name> expand) {

                expansion.add(new Expansion.OfLink(parentSchema, link, parentName.with(name), expand));
                return super.expandLink(name, link, value, expand);
            }

        }, getExpand());
        return expansion;
    }

    @Data
    class OfRef implements Expansion {

        private final LinkableSchema source;

        private final ReferableSchema target;

        private final Name name;

        private final Set<Name> expand;
    }

    @Data
    class OfLink implements Expansion {

        private final LinkableSchema source;

        private final Link link;

        private final Name name;

        private final Set<Name> expand;

        @Override
        public LinkableSchema getTarget() {

            return link.getSchema();
        }
    }
}
