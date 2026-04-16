package com.openmrs.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openmrs.security.JwtAuthenticationFilter;
import com.openmrs.security.JwtUtil;
import com.openmrs.security.SecurityConfig;
import com.openmrs.service.SecretService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(SecretController.class)
@Import({SecurityConfig.class, JwtAuthenticationFilter.class})
class SecretControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private SecretService secretService;

    @MockitoBean
    private JwtUtil jwtUtil;

    private void mockAuth(String role) {
        when(jwtUtil.isValid("test-token")).thenReturn(true);
        when(jwtUtil.getUsername("test-token")).thenReturn("testuser");
        when(jwtUtil.getRole("test-token")).thenReturn(role);
    }

    @Test
    void listSecrets_asAdmin_returnsNames() throws Exception {
        mockAuth("ADMIN");
        when(secretService.listSecretNames()).thenReturn(List.of("db-user", "db-pass"));

        mockMvc.perform(get("/api/secrets")
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0]").value("db-user"))
            .andExpect(jsonPath("$[1]").value("db-pass"));
    }

    @Test
    void listSecrets_asOperator_returns403() throws Exception {
        mockAuth("OPERATOR");

        mockMvc.perform(get("/api/secrets")
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isForbidden());
    }

    @Test
    void createSecret_asAdmin_returns201() throws Exception {
        mockAuth("ADMIN");

        mockMvc.perform(post("/api/secrets")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of(
                    "name", "db-password",
                    "value", "secret123"
                ))))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.name").value("db-password"));

        verify(secretService).createOrUpdate("db-password", "secret123");
    }

    @Test
    void createSecret_missingName_returns400() throws Exception {
        mockAuth("ADMIN");

        mockMvc.perform(post("/api/secrets")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of("value", "secret"))))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void createSecret_missingValue_returns400() throws Exception {
        mockAuth("ADMIN");

        mockMvc.perform(post("/api/secrets")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of("name", "key"))))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void createSecret_invalidNameChars_returns400() throws Exception {
        mockAuth("ADMIN");

        mockMvc.perform(post("/api/secrets")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of(
                    "name", "invalid name!",
                    "value", "secret"
                ))))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void deleteSecret_asAdmin_succeeds() throws Exception {
        mockAuth("ADMIN");

        mockMvc.perform(delete("/api/secrets/db-password")
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true));

        verify(secretService).delete("db-password");
    }

    @Test
    void deleteSecret_notFound_returns404() throws Exception {
        mockAuth("ADMIN");
        doThrow(new IllegalArgumentException("Secret not found: missing"))
            .when(secretService).delete("missing");

        mockMvc.perform(delete("/api/secrets/missing")
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void secretEndpoints_asViewer_returns403() throws Exception {
        mockAuth("VIEWER");

        mockMvc.perform(get("/api/secrets")
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isForbidden());

        mockMvc.perform(post("/api/secrets")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"k\",\"value\":\"v\"}"))
            .andExpect(status().isForbidden());
    }
}