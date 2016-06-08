function putMethod(context, params, input) {
    xdmp.documentInsert(params.uri, input, null, params.collection)
};
exports.PUT    = putMethod;

function doNothing(context, params) {};
exports.GET    = doNothing;
exports.POST   = doNothing;
exports.DELETE = doNothing;
