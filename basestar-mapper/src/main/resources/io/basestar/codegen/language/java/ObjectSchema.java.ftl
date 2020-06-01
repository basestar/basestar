<#import "_macros.ftl" as macros>
package ${packageName};

<@macros.annotation name=annotationClassName values=annotationValues/><#nt>
public class ${className} {

<#list members as member>
    <@macros.annotation name=member.annotationClassName values=member.annotationValues/><#nt><#if member.required>
    @javax.validation.constraints.NotNull</#if>
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
    public boolean equals(final Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ${className} other = (${className}) o;
        return <#list members as member>java.util.Objects.equals(${member.fieldName}, other.${member.fieldName})<#sep>
            && </#list>;
    }

    @Override
    public int hashCode() {

        return java.util.Objects.hash(<#list members as member>${member.fieldName}<#sep>, </#list>);
    }
}
