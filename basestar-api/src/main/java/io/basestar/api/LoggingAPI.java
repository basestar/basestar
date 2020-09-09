//package io.basestar.api;
//
//import io.basestar.util.Nullsafe;
//import io.swagger.v3.oas.models.OpenAPI;
//import lombok.extern.slf4j.Slf4j;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.concurrent.CompletableFuture;
//
//public class LoggingAPI implements API {
//
//    private final API api;
//
//    private final boolean logRequestBodies;
//
//    private final boolean logResponseBodies;
//
//    private final Logger log;
//
//    @lombok.Builder(builderClassName = "Builder")
//    LoggingAPI(final API api, final Boolean logRequestBodies, final Boolean logResponseBodies, final Logger log) {
//
//        this.api = api;
//        this.logRequestBodies = Nullsafe.option(logRequestBodies);
//        this.logResponseBodies = Nullsafe.option(logResponseBodies);
//        this.log = Nullsafe.option(log, () -> LoggerFactory.getLogger(LoggingAPI.class));
//    }
//
//    @Override
//    public CompletableFuture<APIResponse> handle(final APIRequest request) throws IOException {
//
//        return api.handle(request(request)).thenApply(this::response);
//    }
//
//    private APIRequest request(final APIRequest request) {
//
//        final APIRequest result;
//        final String body;
//        if(logRequestBodies) {
//            try(final InputStream is = request.readBody()) {
//                final byte[] bytes = request.getContentType().getMapper().writeValueAsBytes(body.getBody());
//            }
//            result = new APIRequest.Delegating(request) {
//
//            };
//        } else {
//            body = "not logged";
//            result = request;
//        }
//        log.info("Request {} {} Query: {} Headers: {} Body : {}", request.getMethod(), request.getPath(), request.getQuery(), request.getHeaders(), body);
//    }
//
//    private APIResponse response(final APIResponse response) {
//
//        if(logResponseBodies) {
//            return null;
//        } else {
//            log.info();
//        }
//    }
//
//    @Override
//    public CompletableFuture<OpenAPI> openApi() {
//
//        return api.openApi();
//    }
//}
