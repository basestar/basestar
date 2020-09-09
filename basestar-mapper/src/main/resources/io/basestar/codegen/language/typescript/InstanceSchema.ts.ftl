<#import "_macros.ftl" as macros>
<#list schemaDependencies as dep>
<@macros.import dependency=dep/>
</#list>

interface ${className}Props <#if extend??>extends ${extend.className}Props</#if> {

<#list members as member>
    readonly ${member.fieldName}<#if !member.required>?</#if> : <@macros.propsType of=member.type/>;

</#list>
}

<#if abstract>abstract </#if>class ${className} <#if extend??>extends ${extend.className}</#if> {

<#list members as member>
    public ${member.fieldName}<#if !member.required>?</#if> : <@macros.type of=member.type/>;

</#list>
    <#if abstract>protected </#if>constructor(props: ${className}Props) {

        <#if extend??>super(props);</#if>
        if(props) {
<#list members as member>
            this.${member.fieldName} = (props?.${member.fieldName} != null) ? <@macros.construct name='props.' + member.fieldName type=member.type/> : undefined;
</#list>
        }
    }
<#if !abstract>

    static from(props: ${className}Props) : ${className} {

        return new ${className}(props);
    }
<#elseif extend??>

    static from(props: ${className}Props) : ${className} {

        return ${extend.className}.from(props) as ${className};
    }
</#if>
}

export {${className}Props};
export default ${className};
