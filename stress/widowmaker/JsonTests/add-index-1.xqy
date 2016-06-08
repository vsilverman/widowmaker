xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";       
let $config := admin:get-configuration()
let $dbid := xdmp:database("STRESS")
let $rangespec := admin:database-range-element-index("unsignedLong", "", "PMID", (), fn:false() )
let $d := admin:database-add-range-element-index($config, $dbid, $rangespec)
return  admin:save-configuration($d);

xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";       
let $config := admin:get-configuration()
let $dbid := xdmp:database("STRESS")
let $rangespec := admin:database-range-element-index("string", "", "Country", "http://marklogic.com/collation/", fn:false() )
let $d := admin:database-add-range-element-index($config, $dbid, $rangespec)
return  admin:save-configuration($d);

xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";       
let $config := admin:get-configuration()
let $dbid := xdmp:database("STRESS")
let $rangespec := admin:database-range-path-index($dbid, "unsignedInt", "MedlineCitation//JournalIssue/PubDate/Year", (), fn:false(), "reject" )
let $d := admin:database-add-range-path-index($config, $dbid, $rangespec)
return  admin:save-configuration($d);

xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";       
let $config := admin:get-configuration()
let $dbid := xdmp:database("STRESS")
let $rangespec := admin:database-range-path-index($dbid, "string", "/MedlineCitation/Article/Journal/ISSN", "http://marklogic.com/collation/", fn:false(), "reject" )
let $d := admin:database-add-range-path-index($config, $dbid, $rangespec)
return  admin:save-configuration($d);

import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";
declare namespace db = "http://marklogic.com/xdmp/database";
let $config := admin:get-configuration()
let $dbid := xdmp:database("STRESS")
let $field-name := "ISSN"
let $field-paths := admin:database-field-path("Article/Journal/ISSN", 1.0)
let $fieldspec := admin:database-path-field($field-name,$field-paths) 
let $config := admin:database-add-field($config, $dbid, $fieldspec)
let $config := admin:database-set-field-value-searches($config, xdmp:database("STRESS"), "ISSN", fn:true())
return admin:save-configuration($config) 
