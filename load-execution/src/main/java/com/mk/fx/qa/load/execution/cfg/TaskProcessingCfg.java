package com.mk.fx.qa.load.execution.cfg;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@Configuration
@ConfigurationProperties(prefix = "load.tasks")
public class TaskProcessingCfg {

  @Min(1)
  @Max(64)
  private int concurrency = 1;

  @Positive private int historySize = 50;
}
