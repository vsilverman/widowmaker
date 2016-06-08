xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";       
let $config := admin:get-configuration()
let $dbid := xdmp:database("MedlineDB")
let $nsspec := admin:database-path-namespace("stress","http://www.mediawiki.org/xml/export-0.4/")
return  admin:save-configuration(admin:database-add-path-namespace($config, $dbid, $nsspec));
 
xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy"; 
let $config := admin:get-configuration()
let $dbid := xdmp:database("MedlineDB")
let $pRangespec := 
  admin:database-range-path-index($dbid,
                                  "string",
                                  "//stress:a/@href",
                                  "http://marklogic.com/collation/",
                                  xs:boolean("false"),
                                  "ignore")
return admin:save-configuration(admin:database-add-range-path-index($config, $dbid, $pRangespec));

xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy"; 
let $config := admin:get-configuration()
let $dbid := xdmp:database("MedlineDB")
let $pRangespec := 
  admin:database-range-path-index($dbid,
                                  "string",
                                  "stress:li/stress:a",
                                  "http://marklogic.com/collation/",
                                  xs:boolean("false"),
                                  "ignore")
return admin:save-configuration(admin:database-add-range-path-index($config, $dbid, $pRangespec));

xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy"; 
let $config := admin:get-configuration()
let $dbid := xdmp:database("MedlineDB")
let $pRangespec := 
  admin:database-range-path-index($dbid,
                                  "string",
                                  "stress:a",
                                  "http://marklogic.com/collation/",
                                  xs:boolean("false"),
                                  "ignore")
return admin:save-configuration(admin:database-add-range-path-index($config, $dbid, $pRangespec));

xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy"; 
let $config := admin:get-configuration()
let $dbid := xdmp:database("MedlineDB")
let $pRangespec := 
  admin:database-range-path-index($dbid,
                                  "string",
                                  "/stress:page/*/*/stress:table/*/stress:td[stress:div/@*='toctitle']/stress:ul/stress:ul/stress:li[@class='toclevel-1']/stress:a",
                                  "http://marklogic.com/collation/",
                                  xs:boolean("false"),
                                  "ignore")
return admin:save-configuration(admin:database-add-range-path-index($config, $dbid, $pRangespec));

xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy"; 
let $config := admin:get-configuration()
let $dbid := xdmp:database("MedlineDB")
let $pRangespec := 
  admin:database-range-path-index($dbid,
                                  "string",
                                  "/stress:page/*:revision/stress:*/stress:p/stress:a/@title",
                                  "http://marklogic.com/collation/",
                                  xs:boolean("false"),
                                  "ignore")
return admin:save-configuration(admin:database-add-range-path-index($config, $dbid, $pRangespec));

xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy"; 
let $config := admin:get-configuration()
let $dbid := xdmp:database("MedlineDB")
let $pRangespec := 
  admin:database-range-path-index($dbid,
                                  "string",
                                  "/stress:page/*:revision/stress:*/stress:p/stress:b/*:a[@href='Norway_in_1814']",
                                  "http://marklogic.com/collation/",
                                  xs:boolean("false"),
                                  "ignore")
return admin:save-configuration(admin:database-add-range-path-index($config, $dbid, $pRangespec))
