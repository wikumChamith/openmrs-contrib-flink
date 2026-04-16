package com.openmrs.controller;

import com.openmrs.model.Role;
import com.openmrs.model.UserAccount;
import com.openmrs.repository.UserAccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

    private final UserAccountRepository userAccountRepository;
    private final PasswordEncoder passwordEncoder;

    @GetMapping
    public ResponseEntity<List<Map<String, Object>>> listUsers() {
        List<Map<String, Object>> users = userAccountRepository.findAll().stream()
                .map(u -> Map.<String, Object>of(
                        "id", u.getId(),
                        "username", u.getUsername(),
                        "role", u.getRole().name(),
                        "createdAt", u.getCreatedAt().toString()
                ))
                .toList();
        return ResponseEntity.ok(users);
    }

    @PostMapping
    public ResponseEntity<?> createUser(@RequestBody Map<String, String> body) {
        String username = body.get("username");
        String password = body.get("password");
        String roleStr = body.get("role");

        if (username == null || username.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Username is required"));
        }
        if (password == null || password.length() < 6) {
            return ResponseEntity.badRequest().body(Map.of("error", "Password must be at least 6 characters"));
        }
        if (roleStr == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Role is required (VIEWER, OPERATOR, ADMIN)"));
        }

        Role role;
        try {
            role = Role.valueOf(roleStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid role. Must be VIEWER, OPERATOR, or ADMIN"));
        }

        if (userAccountRepository.existsByUsername(username)) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", "Username already exists"));
        }

        UserAccount user = UserAccount.builder()
                .username(username)
                .password(passwordEncoder.encode(password))
                .role(role)
                .build();
        userAccountRepository.save(user);

        log.info("User '{}' created with role {}", username, role);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "success", true,
                "username", username,
                "role", role.name()
        ));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteUser(@PathVariable Integer id) {
        return userAccountRepository.findById(id)
                .map(user -> {
                    userAccountRepository.delete(user);
                    log.info("User '{}' deleted", user.getUsername());
                    return ResponseEntity.ok(Map.of("success", true, "username", user.getUsername()));
                })
                .orElse(ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "User not found")));
    }
}