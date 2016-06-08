xquery version "1.0-ml";

module namespace getElements = "http://marklogic.com/rest-api/resource/getElements";

declare default function namespace "http://www.w3.org/2005/xpath-functions";

declare option xdmp:mapping "false";

declare function getElements:get(
  $context as map:map,
  $params as map:map
) as document-node()*
{
  let $collection :=  map:get($params, "collection")
  let $doc        :=  map:get($params, "doc")
  let $child-name :=  map:get($params, "child-element")
  let $count      := count(
      switch ($child-name)
      case "patch-ele" return
          if (exists($doc))
          then doc($doc)//patch-ele
          else if (exists($collection))
          then collection($collection)//patch-ele
          else collection()//patch-ele
      case "put-transform-ele" return
          if (exists($doc))
          then doc($doc)//put-transform-ele
          else if (exists($collection))
          then collection($collection)//put-transform-ele
          else collection()//put-transform-ele
      case "update-ele" return
          if (exists($doc))
          then doc($doc)//update-ele
          else if (exists($collection))
          then collection($collection)//update-ele
          else collection()//update-ele
      default return error((),
          "STRESS-TESTERR", "Unknown child element "||$child-name
          )
      )
  return (
    map:put($context, "output-types", "text/plain"),
    document {text {string($count)}}
    )
};
