xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";       
let $config := admin:get-configuration()
let $dbid := xdmp:database("STRESS")
let $rangespec := admin:database-range-element-index("unsignedLong", "", "level", (), fn:false() )
let $d := admin:database-add-range-element-index($config, $dbid, $rangespec)
return  admin:save-configuration($d);

xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";       
let $config := admin:get-configuration()
let $dbid := xdmp:database("STRESS")
let $rangespec := admin:database-range-path-index($dbid, "unsignedInt", "//code", (), fn:false(), "reject" )
let $d := admin:database-add-range-path-index($config, $dbid, $rangespec)
return  admin:save-configuration($d);

xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";       
let $config := admin:get-configuration()
let $dbid := xdmp:database("STRESS")
let $rangespec := admin:database-range-path-index($dbid, "string", "Noproblems/Health/nutrition/nutritionClasses/className/associatedfacts/strength", "http://marklogic.com/collation/", fn:false(), "reject" )
let $d := admin:database-add-range-path-index($config, $dbid, $rangespec)
return  admin:save-configuration($d)
