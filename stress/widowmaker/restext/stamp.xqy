xquery version "1.0-ml";

module namespace stamp = "http://marklogic.com/rest-api/transform/stamp";

declare default function namespace "http://www.w3.org/2005/xpath-functions";
declare option xdmp:mapping "false";

declare function stamp:transform(
    $context as map:map,
    $params  as map:map,
    $content as document-node()  
) as document-node()?
{
    if (empty($content/*)) then $content
    else (
        map:put($context,"output-type","application/xml"),

        let $stamp-name  := (map:get($params,"name"), "transform-timestamp")[1]
        let $stamp-value :=
            let $value := map:get($params,"value")
            return
                if (exists($value) and $value ne "")
                then $value
                else current-dateTime()
        return document {$content/node()/(
            if (. instance of element())
            then element {node-name(.)} {
                namespace::*,
                @*,
                node(),
                element {$stamp-name} {$stamp-value}
                }
            else .
            )}
    )
};
