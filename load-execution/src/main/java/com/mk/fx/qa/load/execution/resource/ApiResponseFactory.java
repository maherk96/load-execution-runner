package com.mk.fx.qa.load.execution.resource;

import com.mk.fx.qa.load.execution.cfg.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

@Component
public class ApiResponseFactory {

  public ResponseEntity<ErrorResponse> error(HttpStatus status, String title, String message) {
    return ResponseEntity.status(status).body(new ErrorResponse(title, message));
  }

  public <T> ResponseEntity<T> ok(T body) {
    return ResponseEntity.ok(body);
  }

  public <T> ResponseEntity<T> accepted(T body) {
    return ResponseEntity.status(HttpStatus.ACCEPTED).body(body);
  }

  public <T> ResponseEntity<T> unavailable(T body) {
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
  }
}
