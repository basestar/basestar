<#import "_macros.ftl" as macros>
package ${packageName};

<#list annotations as annot>
<@macros.annotation name=annot.className values=annot.values/><#nt>
</#list>
public class ${className} <#if extend??>extends ${extend.className}</#if> {

<#list members as member>
    <#list member.annotations as annot>
    <@macros.annotation name=annot.className values=annot.values/><#nt>
    </#list>
    private <@macros.type of=member.type/> ${member.fieldName};

</#list>
<#list members as member>
    public <@macros.type of=member.type/> get${member.name?cap_first}() {

        return ${member.fieldName};
    }

    public ${className} set${member.name?cap_first}(final <@macros.type of=member.type/> ${member.fieldName}) {

        this.${member.fieldName} = ${member.fieldName};
        return this;
    }

</#list>
    @Override
    public String toString() {
        return "${className}{<#list members as member>${member.name}=" + ${member.name}<#sep>
            + ", </#list>
            + "}";
    }

    protected boolean canEqual(final Object other) {

        return other instanceof ${className};
    }

    @Override
    public boolean equals(final Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ${className} other = (${className}) o;
        <#if extend??>
        if (!other.canEqual(this)) return false;
        if (!super.equals(o)) return false;
        </#if>
        return <#list members as member>java.util.Objects.equals(${member.fieldName}, other.${member.fieldName})<#sep>
            && </#list>;
    }

    @Override
    public int hashCode() {

        return java.util.Objects.hash(<#if extend??>super.hashCode(), </#if><#list members as member>${member.fieldName}<#sep>, </#list>);
    }
}
