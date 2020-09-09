<#macro type of><#switch of.name>
    <#case "Array"><@type of=of.type/>[]<#break>
    <#case "Binary">ArrayBuffer<#break>
    <#case "Boolean">boolean<#break>
    <#case "Integer">number<#break>
    <#case "Map">{[key: string]: <@type of=of.type/>}<#break>
    <#case "Number">number<#break>
    <#case "Set"><@type of=of.type/>[]<#break>
    <#case "String">string<#break>
    <#case "Date">Date<#break>
    <#case "DateTime">Date<#break>
    <#case "Any">any<#break>
    <#default>${of.schema.className}<#break>
</#switch></#macro>

<#macro propsType of><#switch of.name>
    <#case "Array"><@propsType of=of.type/>[]<#break>
    <#case "Map">{[key: string]: <@propsType of=of.type/>}<#break>
    <#case "Set"><@propsType of=of.type/>[]<#break>
    <#default><#if of.schema??><#switch of.schema.schemaType>
        <#case "enum">${of.schema.className}<#break>
        <#default>${of.schema.className}Props<#break>
    </#switch><#else><@type of=of/></#if><#break>
</#switch></#macro>

<#macro import dependency>
<#switch dependency.schemaType>
    <#case "enum">import ${dependency.className} from "./${dependency.relativeClassFile}";
<#break>
    <#default>import ${dependency.className}, {${dependency.className}Props} from "./${dependency.relativeClassFile}";
<#break>
</#switch>
</#macro>

<#macro construct name, type, depth=0><#switch type.name>
    <#case "Array">(${name} || []).map(v${depth} => <@construct name='v' + depth type=type.type depth=depth + 1/>)<#break>
    <#case "Map">Object.entries(${name} || {}).map(e${depth} => ({[e${depth}[0]]: <@construct name='e' + depth + '[1]' type=type.type depth=depth + 1/>})).reduce((a${depth}, b${depth}) => ({...a${depth}, ...b${depth}}), {})<#break>
    <#case "Set">(${name} || []).map(v${depth} => <@construct name='v' + depth type=type.type depth=depth + 1/>)<#break>
    <#default><#if type.schema??><#switch type.schema.schemaType>
        <#case "enum">${name}<#break>
        <#default>${type.schema.className}.from(${name})<#break>
    </#switch><#else>${name}</#if><#break>
</#switch></#macro>


.map(e0 => ({[e0[0]]: Property.from(e0[1])})).reduce((a, b) => ({...a, ...b}), {})