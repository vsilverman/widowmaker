xquery version '1.0-ml';
         
let $url := "http://localhost:8002/manage/v2/forests"

let $payload :=
xdmp:quote(
'<forest-create xmlns="http://marklogic.com/manage">
    <forest-name>Documents-MA-2</forest-name>
    <host>rh5-intel64-3.marklogic.com</host> 
    <database>Documents</database>
    <data-directory>/space</data-directory>
    <forest-replicas>
        <forest-replica>
            <replica-name>Documents-RA-2</replica-name>
            <host>rh6-intel64-8.marklogic.com</host>
            <data-directory>/space</data-directory>
        </forest-replica>     
    </forest-replicas>
  </forest-create>'
)

let $options :=
  <options xmlns="xdmp:http">
    <authentication method="digest">
      <username>admin</username>
      <password>admin</password>
    </authentication>
    <data>{$payload}</data>
    <headers>
      <content-type>application/xml</content-type>
    </headers>
  </options>
         
let $result := xdmp:http-post($url, $options)
return $result
