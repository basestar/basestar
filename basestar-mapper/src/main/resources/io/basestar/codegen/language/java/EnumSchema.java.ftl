<#import "_macros.ftl" as macros>
package ${packageName};

<#list annotations as annot>
<@macros.annotation name=annot.className values=annot.values/><#nt>
</#list>
public enum ${className} {
<#list values as value>
    ${value}<#sep>,
</#list>

}
