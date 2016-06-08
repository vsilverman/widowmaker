xquery version "1.0-ml";

module namespace updateDoc = "http://marklogic.com/rest-api/resource/updateDoc";

declare default function namespace "http://www.w3.org/2005/xpath-functions";

declare option xdmp:mapping "false";

declare function updateDoc:post(
  $context as map:map,
  $params as map:map,
  $input as document-node()*
) as document-node()*
{
  let $doc    := map:get($params, "doc")
  let $text   := map:get($params, "text")
  let $update := xdmp:node-insert-child(doc($doc)/*, <update-ele>{$text}</update-ele>)
  let $result :=
    document {
      text {
        "OK"
      }
    }
  return
    (map:put($context, "output-types", "text/plain"), $result)
};
