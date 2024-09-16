package com.trendyol.kafka.stream.api.infra;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.servers.Server;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
@RequiredArgsConstructor
public class OpenApiConfig {
    private final Environment env;

    @Bean
    public OpenAPI apiInfo() {
        Server server = new Server();
        server.setUrl(env.getRequiredProperty("swagger.host.url"));

        return new OpenAPI()
                .addServersItem(server)
                .info(
                        new io.swagger.v3.oas.models.info.Info()
                                .title(env.getRequiredProperty("spring.application.name"))
                                .description(env.getRequiredProperty("swagger.description"))
                                .contact(new io.swagger.v3.oas.models.info.Contact()
                                        .email(env.getRequiredProperty("swagger.contact.email"))
                                        .name(env.getRequiredProperty("swagger.contact.name"))
                                        .url(env.getRequiredProperty("swagger.contact.url")))
                                .license(new io.swagger.v3.oas.models.info.License()
                                        .name(env.getRequiredProperty("swagger.license.name"))
                                        .url(env.getRequiredProperty("swagger.license.url")))
                                .version(env.getRequiredProperty("swagger.version"))
                                .termsOfService(env.getRequiredProperty("swagger.termsOfService"))

                );
    }

    @Bean
    public GroupedOpenApi topicApi() {
        return GroupedOpenApi.builder()
                .group("Topic Services")
                .pathsToMatch("/topics/**")
                .build();
    }


    @Bean
    public GroupedOpenApi consumerApi() {
        return GroupedOpenApi.builder()
                .group("Consumer Services")
                .pathsToMatch("/consumers/**")
                .build();
    }

    @Bean
    public GroupedOpenApi operationApi() {
        return GroupedOpenApi.builder()
                .group("Operations")
                .pathsToMatch("/operations/**")
                .build();
    }
}
