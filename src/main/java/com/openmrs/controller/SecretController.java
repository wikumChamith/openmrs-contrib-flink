package com.openmrs.controller;

import com.openmrs.service.SecretService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/secrets")
@RequiredArgsConstructor
public class SecretController {

    private final SecretService secretService;

    /**
     * List all secret names (values are never exposed)
     */
    @GetMapping
    public ResponseEntity<List<String>> listSecrets() {
        return ResponseEntity.ok(secretService.listSecretNames());
    }

    /**
     * Create or update a secret
     */
    @PostMapping
    public ResponseEntity<?> createSecret(@RequestBody Map<String, String> body) {
        String name = body.get("name");
        String value = body.get("value");

        if (name == null || name.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Secret name is required"));
        }
        if (value == null || value.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Secret value is required"));
        }
        if (!name.matches("[\\w.-]+")) {
            return ResponseEntity.badRequest().body(Map.of("error",
                    "Secret name can only contain letters, digits, underscores, hyphens, and dots"));
        }

        secretService.createOrUpdate(name, value);
        log.info("Secret '{}' created/updated", name);

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("name", name);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * Delete a secret by name
     */
    @DeleteMapping("/{name}")
    public ResponseEntity<?> deleteSecret(@PathVariable String name) {
        try {
            secretService.delete(name);
            log.info("Secret '{}' deleted", name);
            return ResponseEntity.ok(Map.of("success", true, "name", name));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", e.getMessage()));
        }
    }
}