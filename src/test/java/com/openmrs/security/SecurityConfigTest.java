package com.openmrs.security;

import com.openmrs.controller.AuthController;
import com.openmrs.controller.JobController;
import com.openmrs.controller.SecretController;
import com.openmrs.controller.UserController;
import com.openmrs.repository.UserAccountRepository;
import com.openmrs.service.FlinkJobService;
import com.openmrs.service.SecretService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest({AuthController.class, JobController.class, SecretController.class, UserController.class})
@Import({SecurityConfig.class, JwtAuthenticationFilter.class})
class SecurityConfigTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private JwtUtil jwtUtil;

    @MockitoBean
    private FlinkJobService flinkJobService;

    @MockitoBean
    private SecretService secretService;

    @MockitoBean
    private UserAccountRepository userAccountRepository;

    @MockitoBean
    private PasswordEncoder passwordEncoder;

    private void mockAuth(String role) {
        when(jwtUtil.isValid("token")).thenReturn(true);
        when(jwtUtil.getUsername("token")).thenReturn("user");
        when(jwtUtil.getRole("token")).thenReturn(role);
    }

    // --- Public endpoints ---

    @Test
    void authLogin_isPublic() throws Exception {
        mockMvc.perform(post("/api/auth/login")
                .contentType("application/json")
                .content("{\"username\":\"a\",\"password\":\"b\"}"))
            .andExpect(status().isUnauthorized()); // 401 from bad credentials, not 403
    }

    @Test
    void actuatorHealth_isPublic() throws Exception {
        // Actuator endpoints are not loaded in @WebMvcTest slices,
        // so we just verify it doesn't require authentication (returns 404, not 403)
        mockMvc.perform(get("/actuator/health"))
            .andExpect(status().isNotFound());
    }

    // --- Jobs: role-based access ---

    @Test
    void getJobs_viewer_allowed() throws Exception {
        mockAuth("VIEWER");
        mockMvc.perform(get("/api/jobs").header("Authorization", "Bearer token"))
            .andExpect(status().isOk());
    }

    @Test
    void getJobs_operator_allowed() throws Exception {
        mockAuth("OPERATOR");
        mockMvc.perform(get("/api/jobs").header("Authorization", "Bearer token"))
            .andExpect(status().isOk());
    }

    @Test
    void getJobs_admin_allowed() throws Exception {
        mockAuth("ADMIN");
        mockMvc.perform(get("/api/jobs").header("Authorization", "Bearer token"))
            .andExpect(status().isOk());
    }

    @Test
    void getJobs_unauthenticated_forbidden() throws Exception {
        mockMvc.perform(get("/api/jobs"))
            .andExpect(status().isForbidden());
    }

    // --- Secrets: ADMIN only ---

    @Test
    void getSecrets_admin_allowed() throws Exception {
        mockAuth("ADMIN");
        mockMvc.perform(get("/api/secrets").header("Authorization", "Bearer token"))
            .andExpect(status().isOk());
    }

    @Test
    void getSecrets_operator_forbidden() throws Exception {
        mockAuth("OPERATOR");
        mockMvc.perform(get("/api/secrets").header("Authorization", "Bearer token"))
            .andExpect(status().isForbidden());
    }

    @Test
    void getSecrets_viewer_forbidden() throws Exception {
        mockAuth("VIEWER");
        mockMvc.perform(get("/api/secrets").header("Authorization", "Bearer token"))
            .andExpect(status().isForbidden());
    }

    // --- Users: ADMIN only ---

    @Test
    void getUsers_admin_allowed() throws Exception {
        mockAuth("ADMIN");
        mockMvc.perform(get("/api/users").header("Authorization", "Bearer token"))
            .andExpect(status().isOk());
    }

    @Test
    void getUsers_operator_forbidden() throws Exception {
        mockAuth("OPERATOR");
        mockMvc.perform(get("/api/users").header("Authorization", "Bearer token"))
            .andExpect(status().isForbidden());
    }

    @Test
    void getUsers_viewer_forbidden() throws Exception {
        mockAuth("VIEWER");
        mockMvc.perform(get("/api/users").header("Authorization", "Bearer token"))
            .andExpect(status().isForbidden());
    }
}