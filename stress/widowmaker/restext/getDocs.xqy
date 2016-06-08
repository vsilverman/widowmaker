xquery version "1.0-ml";

module namespace getDocs = "http://marklogic.com/rest-api/resource/getDocs";

declare default function namespace "http://www.w3.org/2005/xpath-functions";

declare option xdmp:mapping "false";

declare function getDocs:get(
  $context as map:map,
  $params as map:map
) as document-node()*
{
  let $collection := map:get($params, "collection")
  let $doc        := map:get($params, "doc")
  let $count
    := if (exists($doc))
       then count(doc($doc))
       else count(collection($collection))
  let $result :=
    document {
      text {
        concat($count)
      }
    }
  return
    (map:put($context, "output-types", "text/plain"), $result)
};
