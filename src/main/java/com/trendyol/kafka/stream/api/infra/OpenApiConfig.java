package com.trendyol.kafka.stream.api.infra;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.servers.Server;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

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
                .addOperationCustomizer((operation, handlerMethod) -> {
                    operation.addParametersItem(new Parameter()
                            .in("header")
                            .name("x-project-id")
                            .required(true)
                            .description("Project Id")
                            .schema(new Schema<>().type("string").example("project-x")));

                    operation.addParametersItem(new Parameter()
                            .in("header")
                            .name("x-cluster-id")
                            .required(true)
                            .description("Cluster Id")
                            .schema(new Schema<>().type("string").example("clusterid")));

                    return operation;
                })
                .build();
    }


    @Bean
    public GroupedOpenApi consumerApi() {
        return GroupedOpenApi.builder()
                .group("Consumer Services")
                .pathsToMatch("/consumers/**")
                .addOperationCustomizer((operation, handlerMethod) -> {
                    operation.addParametersItem(new Parameter()
                            .in("header")
                            .name("x-project-id")
                            .required(true)
                            .description("Project Id")
                            .schema(new Schema<>().type("string").example("project-x")));

                    operation.addParametersItem(new Parameter()
                            .in("header")
                            .name("x-cluster-id")
                            .required(true)
                            .description("Cluster Id")
                            .schema(new Schema<>().type("string").example("clusterid")));

                    return operation;
                })
                .build();
    }

    @Bean
    public GroupedOpenApi operationApi() {
        return GroupedOpenApi.builder()
                .group("Operations")
                .pathsToMatch("/operations/**")
                .addOperationCustomizer((operation, handlerMethod) -> {
                    operation.addParametersItem(new Parameter()
                            .in("header")
                            .name("x-project-id")
                            .required(true)
                            .description("Project Id")
                            .schema(new Schema<>().type("string").example("project-x")));

                    operation.addParametersItem(new Parameter()
                            .in("header")
                            .name("x-cluster-id")
                            .required(true)
                            .description("Cluster Id")
                            .schema(new Schema<>().type("string").example("clusterid")));

                    return operation;
                })
                .build();
    }

    @Bean
    public GroupedOpenApi searchApi() {
        return GroupedOpenApi.builder()
                .group("Search")
                .pathsToMatch("/search/**")
                .addOperationCustomizer((operation, handlerMethod) -> {
                    operation.addParametersItem(new Parameter()
                            .in("header")
                            .name("x-project-id")
                            .required(true)
                            .description("Project Id")
                            .schema(new Schema<>().type("string").example("project-x")));

                    operation.addParametersItem(new Parameter()
                            .in("header")
                            .name("x-cluster-id")
                            .required(true)
                            .description("Cluster Id")
                            .schema(new Schema<>().type("string").example("clusterid")));

                    return operation;
                })
                .build();
    }


//    @RestController
//    @RequestMapping("/")
//    static class IndexController {
//        @GetMapping
//        @Operation(hidden = true)
//        public RedirectView redirectToSwaggerUi() {
//            return new RedirectView("/swagger-ui.html");
//        }
//    }
}
