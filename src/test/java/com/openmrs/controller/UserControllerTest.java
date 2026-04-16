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

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(UserController.class)
@Import({SecurityConfig.class, JwtAuthenticationFilter.class})
class UserControllerTest {

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

    private void mockAuth(String role) {
        when(jwtUtil.isValid("test-token")).thenReturn(true);
        when(jwtUtil.getUsername("test-token")).thenReturn("testuser");
        when(jwtUtil.getRole("test-token")).thenReturn(role);
    }

    @Test
    void listUsers_asAdmin_returnsUsers() throws Exception {
        mockAuth("ADMIN");

        UserAccount user = UserAccount.builder()
            .id(1)
            .username("admin")
            .password("encoded")
            .role(Role.ADMIN)
            .createdAt(LocalDateTime.of(2025, 1, 1, 0, 0))
            .build();

        when(userAccountRepository.findAll()).thenReturn(List.of(user));

        mockMvc.perform(get("/api/users")
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].username").value("admin"))
            .andExpect(jsonPath("$[0].role").value("ADMIN"))
            .andExpect(jsonPath("$[0].id").value(1));
    }

    @Test
    void listUsers_asOperator_returns403() throws Exception {
        mockAuth("OPERATOR");

        mockMvc.perform(get("/api/users")
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isForbidden());
    }

    @Test
    void createUser_asAdmin_succeeds() throws Exception {
        mockAuth("ADMIN");

        when(userAccountRepository.existsByUsername("newuser")).thenReturn(false);
        when(passwordEncoder.encode("password123")).thenReturn("encoded");
        when(userAccountRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        mockMvc.perform(post("/api/users")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of(
                    "username", "newuser",
                    "password", "password123",
                    "role", "OPERATOR"
                ))))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.username").value("newuser"))
            .andExpect(jsonPath("$.role").value("OPERATOR"));
    }

    @Test
    void createUser_duplicateUsername_returns409() throws Exception {
        mockAuth("ADMIN");

        when(userAccountRepository.existsByUsername("admin")).thenReturn(true);

        mockMvc.perform(post("/api/users")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of(
                    "username", "admin",
                    "password", "password123",
                    "role", "VIEWER"
                ))))
            .andExpect(status().isConflict())
            .andExpect(jsonPath("$.error").value("Username already exists"));
    }

    @Test
    void createUser_shortPassword_returns400() throws Exception {
        mockAuth("ADMIN");

        mockMvc.perform(post("/api/users")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of(
                    "username", "newuser",
                    "password", "short",
                    "role", "VIEWER"
                ))))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").value("Password must be at least 6 characters"));
    }

    @Test
    void createUser_invalidRole_returns400() throws Exception {
        mockAuth("ADMIN");

        mockMvc.perform(post("/api/users")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of(
                    "username", "newuser",
                    "password", "password123",
                    "role", "SUPERADMIN"
                ))))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void createUser_missingUsername_returns400() throws Exception {
        mockAuth("ADMIN");

        mockMvc.perform(post("/api/users")
                .header("Authorization", "Bearer test-token")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Map.of(
                    "password", "password123",
                    "role", "VIEWER"
                ))))
            .andExpect(status().isBadRequest());
    }

    @Test
    void deleteUser_asAdmin_succeeds() throws Exception {
        mockAuth("ADMIN");

        UserAccount user = UserAccount.builder()
            .id(1)
            .username("toDelete")
            .role(Role.VIEWER)
            .build();

        when(userAccountRepository.findById(1)).thenReturn(Optional.of(user));

        mockMvc.perform(delete("/api/users/1")
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true));

        verify(userAccountRepository).delete(user);
    }

    @Test
    void deleteUser_notFound_returns404() throws Exception {
        mockAuth("ADMIN");

        when(userAccountRepository.findById(999)).thenReturn(Optional.empty());

        mockMvc.perform(delete("/api/users/999")
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isNotFound());
    }
}