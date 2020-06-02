<#macro type of><#switch of.name>
    <#case "Array">java.util.List<<@type of=of.type/>><#break>
    <#case "Binary">byte[]<#break>
    <#case "Boolean">Boolean<#break>
    <#case "Integer">Long<#break>
    <#case "Map">java.util.Map<String, <@type of=of.type/>><#break>
    <#case "Number">Double<#break>
    <#case "Set">java.util.Set<<@type of=of.type/>><#break>
    <#case "String">String<#break>
    <#default>${of.schema.className}<#break>
</#switch></#macro>

<#macro value of><#if of?is_string>"${of?j_string}"<#else>${of}</#if></#macro>

<#macro annotation name values>@${name}<#if values?has_content>(<#list values as k,v>${k} = <@value of=v/><#sep>, </#list>)</#if></#macro>