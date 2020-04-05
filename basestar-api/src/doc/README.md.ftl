# API interface

Adaptor interface for exposing services over a HTTP-like API.

Request and response implementations will generally live in Connector modules, and API implementations
will live in the same module as the service(s) they provide an API for.

Reflection-based mapping of APIs is generally discouraged, this layer is supposed to be light enough to
run in an AWS lambda.


