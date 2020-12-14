<#macro type of><#switch of.name>
    <#case "Array">java.util.List<<@type of=of.type/>><#break>
    <#case "Binary">byte[]<#break>
    <#case "Boolean">Boolean<#break>
    <#case "Integer">Long<#break>
    <#case "Map">java.util.Map<String, <@type of=of.type/>><#break>
    <#case "Number">Double<#break>
    <#case "Set">java.util.Set<<@type of=of.type/>><#break>
    <#case "String">String<#break>
    <#case "Date">java.time.LocalDate<#break>
    <#case "DateTime">java.time.Instant<#break>
    <#case "Any">Object<#break>
    <#case "Page">io.basestar.util.Page<<@type of=of.type/>><#break>
    <#case "Secret">io.basestar.secret.Secret<#break>
    <#default>${of.schema.fullyQualifiedClassName}<#break>
</#switch></#macro>

<#macro values of>{<#list of as v><@value of=v/><#sep>, </#list>}</#macro>
<#macro value of><#if of?is_hash><@object of=of/><#elseif of?is_string>"${of?j_string}"<#elseif of?is_boolean>${of?c}<#elseif of?is_sequence><@values of=of/><#else>${of}</#if></#macro>

<#macro object of><#if of.className??><@annotation name=of.className values=of.values/><#else>${of}</#if></#macro>
<#macro annotation name values>@${name}<#if values?has_content>(<#list values as k,v>${k} = <@value of=v/><#sep>, </#list>)</#if></#macro>

<#macro extend schemas><#if schemas?has_content> implements <#list schemas as v>${v.className}<#sep>, </#list></#if></#macro>
