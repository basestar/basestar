<#import "_macros.ftl" as macros>

enum ${className} {
<#list values as value>
    ${value}<#sep>,
</#list>

}

export default ${className};
