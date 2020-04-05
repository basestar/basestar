//package io.basestar.schema;
//
//import com.fasterxml.jackson.annotation.JsonIgnore;
//import io.basestar.util.Path;
//import lombok.Data;
//import lombok.experimental.Accessors;
//
//import java.util.Set;
//
//@Data
//@Accessors(chain = true)
//public class Method implements Member {
//
//    @JsonIgnore
//    private String name;
//
//    @Override
//    public void resolve(final String name, final Schema schema, final Namespace namespace) {
//
//        this.name = name;
//    }
//
//    @Override
//    public Object expand(final Object value, final Expander expander, final Set<Path> expand) {
//
//        return null;
//    }
//
//    @Override
//    public Set<Path> requireExpand(final Set<Path> paths) {
//
//        return null;
//    }
//}
