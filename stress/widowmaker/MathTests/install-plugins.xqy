xquery version "1.0-ml";
import module namespace plugin = 'http://marklogic.com/extension/plugin' at '/MarkLogic/plugin/plugin.xqy';

(: replace this with the correct path on your box :)
let $path := "/space/hwu/xdmp/src/Samples/NativePlugins/sampleplugin.zip"
return plugin:install-from-zip("", xdmp:document-get($path)/node())
