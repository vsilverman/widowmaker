xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";       
let $config := admin:get-configuration()
let $dbid := xdmp:database("MedlineDB")
let $rangespec := admin:database-range-element-index("unsignedLong", "http://marklogic.com/qa", "ulong-1", "http://marklogic.com/collation/", fn:false() )
let $d := admin:database-add-range-element-index($config, $dbid, $rangespec)
return  admin:save-configuration($d);

xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";
let $config := admin:get-configuration()
let $dbid := xdmp:database("MedlineDB")
let $rangespec := admin:database-range-element-index("double", "http://marklogic.com/qa", "double-1", "http://marklogic.com/collation/", fn:false() )
let $d := admin:database-add-range-element-index($config, $dbid, $rangespec)
return  admin:save-configuration($d)

