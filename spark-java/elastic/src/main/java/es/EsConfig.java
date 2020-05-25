package es;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * 使用于基于Spring App
 *
 * @author admin 2020-6-19
 */
@Deprecated
@Configuration
public class EsConfig {
    @Value("${elasticsearch.nodes}")
    private List<String> nodes;

    @Value("${elasticsearch.schema}")
    private String schema;
}
