package com.trendyol.kafka.stream.api.controller.filter;

import com.trendyol.kafka.stream.api.controller.context.RequestContext;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

@Component
public class HeaderCaptureFilter implements Filter {
    protected static final String[] REQUIRED_HEADERS = {"x-project-id", "x-cluster-id"};

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest httpServletRequest && response instanceof HttpServletResponse httpServletResponse) {

            String url = httpServletRequest.getRequestURI();
            if(url.contains("/topics") || url.contains("/consumers") || url.contains("/search")){
                // Validate required headers
                for (String header : REQUIRED_HEADERS) {
                    String rhv = httpServletRequest.getHeader(header);
                    if (rhv == null) {
                        httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                        httpServletResponse.getWriter().write("Missing required header: " + header);
                        return;
                    }
                }
            }

            // Capture headers from the request and store them in MDC
            Map<String, String> headers = new HashMap<>();
            Enumeration<String> headerNames = httpServletRequest.getHeaderNames();

            while (headerNames.hasMoreElements()) {
                String headerName = headerNames.nextElement();
                String headerValue = httpServletRequest.getHeader(headerName);
                headers.put(headerName, headerValue);
                MDC.put(headerName, headerValue);
            }

            // Store headers in RequestContext for ThreadLocal access (if needed)
            RequestContext.setHeaders(headers);
        }

        try {
            chain.doFilter(request, response);
        } finally {
            RequestContext.clear();
            MDC.clear();
        }
    }

    @Override
    public void destroy() {
        // Cleanup code if needed
    }
}