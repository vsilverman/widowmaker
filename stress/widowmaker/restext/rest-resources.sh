AUTH=admin:admin
HOST=$HOST
OPTS="-i -v"

curl $OPTS -X PUT --digest --user $AUTH -H "Content-type: application/xquery" \
  -T getElements.xqy \
  "$HOST/v1/config/resources/getElements?method=get&get:collection=string%3f&get:doc=string%3f&get:root-attr=string%3f&get:value=string%3f"

curl $OPTS -X PUT --digest --user $AUTH -H "Content-type: application/xquery" \
  -T getDocs.xqy \
  "$HOST/v1/config/resources/getDocs?method=get&get:collection=string%3f&get:doc=string%3f"

curl $OPTS -X PUT --digest --user $AUTH -H "Content-type: application/xquery" \
  -T getURIs.xqy \
  "$HOST/v1/config/resources/getURIs?method=get&get:from=integer&get:to=integer&get:collection=string"

curl $OPTS -X PUT --digest --user $AUTH -H "Content-type: application/xquery" \
  -T updateDoc.xqy \
  "$HOST/v1/config/resources/updateDoc?method=post&post:doc=string&post:text=string"

curl $OPTS -X PUT --digest --user $AUTH -H "Content-type: application/xquery" \
  -T stamp.xqy \
  "$HOST/v1/config/transforms/stamp?trans:name=string%3f&trans:value=string%3f"

curl $OPTS -X PUT --digest --user $AUTH -H "Content-type: application/xml" \
  -T options.xml \
  "$HOST/v1/config/query/default"

curl $OPTS -X PUT --digest --user $AUTH -H "Content-type: application/vnd.marklogic-javascript" \
  -T addDoc.sjs \
  "$HOST/v1/config/resources/AddDocResource?method=PUT"
