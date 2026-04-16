package com.openmrs.controller;

import com.openmrs.model.*;
import com.openmrs.security.JwtAuthenticationFilter;
import com.openmrs.security.JwtUtil;
import com.openmrs.security.SecurityConfig;
import com.openmrs.service.FlinkJobService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(JobController.class)
@Import({SecurityConfig.class, JwtAuthenticationFilter.class})
class JobControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private FlinkJobService flinkJobService;

    @MockitoBean
    private JwtUtil jwtUtil;

    private void mockAuth(String role) {
        when(jwtUtil.isValid("test-token")).thenReturn(true);
        when(jwtUtil.getUsername("test-token")).thenReturn("testuser");
        when(jwtUtil.getRole("test-token")).thenReturn(role);
    }

    @Test
    void getAllJobs_asViewer_returnsJobs() throws Exception {
        mockAuth("VIEWER");

        Job job = new Job();
        job.setId(1);
        SourceInfo source = new SourceInfo();
        source.setSourceTable("encounter");
        source.setSourcePassword("secret");
        job.setSource(source);
        SinkInfo sink = new SinkInfo();
        sink.setSinkTable("flat_encounter");
        sink.setSinkPassword("secret");
        job.setSink(sink);

        when(flinkJobService.getAllJobs()).thenReturn(List.of(job));

        mockMvc.perform(get("/api/jobs")
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].id").value(1))
            .andExpect(jsonPath("$[0].source.sourcePassword").value("******"))
            .andExpect(jsonPath("$[0].sink.sinkPassword").value("******"));
    }

    @Test
    void getAllJobs_unauthenticated_returns403() throws Exception {
        mockMvc.perform(get("/api/jobs"))
            .andExpect(status().isForbidden());
    }

    @Test
    void uploadYaml_asOperator_succeeds() throws Exception {
        mockAuth("OPERATOR");

        Job job = new Job();
        job.setId(1);
        SourceInfo source = new SourceInfo();
        source.setSourceTable("encounter");
        job.setSource(source);
        SinkInfo sink = new SinkInfo();
        sink.setSinkTable("flat_encounter");
        job.setSink(sink);

        when(flinkJobService.registerJobFromYaml(any())).thenReturn(job);

        MockMultipartFile file = new MockMultipartFile(
            "file", "vitals.yaml", "text/yaml", "sourceTable: encounter".getBytes());

        mockMvc.perform(multipart("/api/jobs/upload").file(file)
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.jobId").value(1));
    }

    @Test
    void uploadYaml_asViewer_returns403() throws Exception {
        mockAuth("VIEWER");

        MockMultipartFile file = new MockMultipartFile(
            "file", "vitals.yaml", "text/yaml", "content".getBytes());

        mockMvc.perform(multipart("/api/jobs/upload").file(file)
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isForbidden());
    }

    @Test
    void uploadYaml_emptyFile_returns400() throws Exception {
        mockAuth("OPERATOR");

        MockMultipartFile file = new MockMultipartFile(
            "file", "vitals.yaml", "text/yaml", new byte[0]);

        mockMvc.perform(multipart("/api/jobs/upload").file(file)
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").value("File is empty"));
    }

    @Test
    void uploadYaml_nonYamlFile_returns400() throws Exception {
        mockAuth("OPERATOR");

        MockMultipartFile file = new MockMultipartFile(
            "file", "data.json", "application/json", "{}".getBytes());

        mockMvc.perform(multipart("/api/jobs/upload").file(file)
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void uploadYaml_ymlExtension_accepted() throws Exception {
        mockAuth("OPERATOR");

        Job job = new Job();
        job.setId(2);
        SourceInfo source = new SourceInfo();
        source.setSourceTable("encounter");
        job.setSource(source);
        SinkInfo sink = new SinkInfo();
        sink.setSinkTable("flat_encounter");
        job.setSink(sink);

        when(flinkJobService.registerJobFromYaml(any())).thenReturn(job);

        MockMultipartFile file = new MockMultipartFile(
            "file", "vitals.yml", "text/yaml", "sourceTable: encounter".getBytes());

        mockMvc.perform(multipart("/api/jobs/upload").file(file)
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isCreated());
    }

    @Test
    void deleteJob_asOperator_succeeds() throws Exception {
        mockAuth("OPERATOR");

        when(flinkJobService.existsById(1)).thenReturn(true);

        mockMvc.perform(delete("/api/jobs/1")
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true));

        verify(flinkJobService).deleteJob(1);
    }

    @Test
    void deleteJob_notFound_returns404() throws Exception {
        mockAuth("OPERATOR");

        when(flinkJobService.existsById(999)).thenReturn(false);

        mockMvc.perform(delete("/api/jobs/999")
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void deleteJob_asViewer_returns403() throws Exception {
        mockAuth("VIEWER");

        mockMvc.perform(delete("/api/jobs/1")
                .header("Authorization", "Bearer test-token"))
            .andExpect(status().isForbidden());
    }
}