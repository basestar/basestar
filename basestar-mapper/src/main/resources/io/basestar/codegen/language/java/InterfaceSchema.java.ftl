<#import "_macros.ftl" as macros>
package ${packageName};

<#list annotations as annot>
<@macros.annotation name=annot.className values=annot.values/><#nt>
</#list>
@com.fasterxml.jackson.databind.annotation.JsonSerialize(using = io.basestar.mapper.jackson.UnmarshallingSerializer.class)
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = io.basestar.mapper.jackson.MarshallingDeserializer.class)
public interface ${className}<@macros.extend schemas=extend/><#nt> {

<#list members as member>
    public <@macros.type of=member.type/> get${member.name?cap_first}();

    public ${className} set${member.name?cap_first}(final <@macros.type of=member.type/> ${member.fieldName});

</#list>
}
