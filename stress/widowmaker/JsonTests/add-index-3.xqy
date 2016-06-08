xquery version "1.0-ml";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";       
let $config := admin:get-configuration()
let $dbid := xdmp:database("STRESS")
let $indexspec :=   admin:database-geospatial-path-index("/geometry[type='MultiPoint']/array-node('coordinates')/array-node('coordinates')","wgs84",fn:false(), "long-lat-point","reject")
let $d := admin:database-add-geospatial-path-index($config, $dbid, $indexspec)
return  admin:save-configuration($d)

