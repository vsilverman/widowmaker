xquery version "1.0-ml";

module namespace getURIs = "http://marklogic.com/rest-api/resource/getURIs";

declare default function namespace "http://www.w3.org/2005/xpath-functions";

declare option xdmp:mapping "false";

declare function getURIs:get(
  $context as map:map,
  $params as map:map
) as document-node()*
{
  let $uris :=
    let $collection := map:get($params, "collection")
    let $from := xs:integer(map:get($params, "from"))
    let $to   := xs:integer(map:get($params, "to"))
    for $doc in collection($collection)[$from to $to]
    return
      base-uri($doc)

  (: N.B. When bug 18524 is fixed, remove dummy-filler and the corresponding
     hack in the Java client
   :)

  let $result :=
    document {
      text {
        string-join(("dummy-filler", $uris), "&#10;")
      }
    }

  (:let $trace := xdmp:log(("URIS:", $result)) :)

  return
    (map:put($context, "output-types", "text/plain"), $result)
};
