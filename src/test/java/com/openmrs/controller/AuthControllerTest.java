package com.openmrs.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openmrs.model.Role;
import com.openmrs.model.UserAccount;
import com.openmrs.repository.UserAccountRepository;
import com.openmrs.security.JwtAuthenticationFilter;
import com.openmrs.security.JwtUtil;
import com.openmrs.security.SecurityConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(AuthController.class)
@Import({SecurityConfig.class, JwtAuthenticationFilter.class})
class AuthControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private UserAccountRepository userAccountRepository;

    @MockitoBean
    private PasswordEncoder passwordEncoder;

    @MockitoBean
    private JwtUtil jwtUtil;

    @Test
    void login_validCredentials_returnsToken() throws Exception {
        UserAccount user = UserAccount.builder()
            .username("admin")
            .password("encoded-password")
            .role(Role.ADMIN)
            .build();

        when(userAccountRepository.findByUsername("admin")).thenReturn(Optional.of(user));
        when(passwordEncoder.matches("admin123", "encoded-password")).thenReturn(true);
        when(jwtUtil.generateToken("admin", "ADMIN")).thenReturn("test-jwt-token");

        mockMvc.perform(post("/api/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of(
                    "username", "admin",
                    "password", "admin123"
                ))))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.token").value("test-jwt-token"))
            .andExpect(jsonPath("$.username").value("admin"))
            .andExpect(jsonPath("$.role").value("ADMIN"));
    }

    @Test
    void login_invalidPassword_returns401() throws Exception {
        UserAccount user = UserAccount.builder()
            .username("admin")
            .password("encoded-password")
            .role(Role.ADMIN)
            .build();

        when(userAccountRepository.findByUsername("admin")).thenReturn(Optional.of(user));
        when(passwordEncoder.matches("wrong", "encoded-password")).thenReturn(false);

        mockMvc.perform(post("/api/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of(
                    "username", "admin",
                    "password", "wrong"
                ))))
            .andExpect(status().isUnauthorized())
            .andExpect(jsonPath("$.error").value("Invalid username or password"));
    }

    @Test
    void login_unknownUser_returns401() throws Exception {
        when(userAccountRepository.findByUsername("nobody")).thenReturn(Optional.empty());

        mockMvc.perform(post("/api/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of(
                    "username", "nobody",
                    "password", "pass"
                ))))
            .andExpect(status().isUnauthorized());
    }

    @Test
    void login_missingUsername_returns400() throws Exception {
        mockMvc.perform(post("/api/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of("password", "pass"))))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void login_missingPassword_returns400() throws Exception {
        mockMvc.perform(post("/api/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of("username", "admin"))))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").exists());
    }
}