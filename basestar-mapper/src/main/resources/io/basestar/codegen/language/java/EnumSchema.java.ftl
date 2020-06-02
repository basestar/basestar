<#import "_macros.ftl" as macros>
package ${packageName};

<@macros.annotation name=annotationClassName values=annotationValues/><#nt>
public enum ${className} {
<#list values as value>
    ${value}<#sep>,
</#list>

}
