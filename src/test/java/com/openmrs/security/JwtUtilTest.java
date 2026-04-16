package com.openmrs.security;

import io.jsonwebtoken.Claims;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class JwtUtilTest {

    private JwtUtil jwtUtil;

    // A valid base64-encoded 256-bit key for testing
    private static final String TEST_SECRET = "dGhpcyBpcyBhIHNhbXBsZSBzZWNyZXQga2V5IGZvciBqd3Q=";
    private static final long EXPIRATION_MS = 86400000; // 24 hours

    @BeforeEach
    void setUp() {
        jwtUtil = new JwtUtil(TEST_SECRET, EXPIRATION_MS);
    }

    @Test
    void generateToken_containsUsernameAndRole() {
        String token = jwtUtil.generateToken("admin", "ADMIN");

        assertThat(token).isNotNull().isNotEmpty();
        assertThat(jwtUtil.getUsername(token)).isEqualTo("admin");
        assertThat(jwtUtil.getRole(token)).isEqualTo("ADMIN");
    }

    @Test
    void parseToken_returnsValidClaims() {
        String token = jwtUtil.generateToken("operator", "OPERATOR");

        Claims claims = jwtUtil.parseToken(token);

        assertThat(claims.getSubject()).isEqualTo("operator");
        assertThat(claims.get("role", String.class)).isEqualTo("OPERATOR");
        assertThat(claims.getIssuedAt()).isNotNull();
        assertThat(claims.getExpiration()).isNotNull();
        assertThat(claims.getExpiration()).isAfter(claims.getIssuedAt());
    }

    @Test
    void isValid_validToken_returnsTrue() {
        String token = jwtUtil.generateToken("user", "VIEWER");

        assertThat(jwtUtil.isValid(token)).isTrue();
    }

    @Test
    void isValid_invalidToken_returnsFalse() {
        assertThat(jwtUtil.isValid("not.a.valid.token")).isFalse();
    }

    @Test
    void isValid_tamperedToken_returnsFalse() {
        String token = jwtUtil.generateToken("admin", "ADMIN");
        String tampered = token.substring(0, token.length() - 5) + "XXXXX";

        assertThat(jwtUtil.isValid(tampered)).isFalse();
    }

    @Test
    void isValid_expiredToken_returnsFalse() {
        // Create a JwtUtil with 0ms expiration
        JwtUtil expiredJwtUtil = new JwtUtil(TEST_SECRET, 0);
        String token = expiredJwtUtil.generateToken("admin", "ADMIN");

        assertThat(expiredJwtUtil.isValid(token)).isFalse();
    }

    @Test
    void isValid_differentKey_returnsFalse() {
        String token = jwtUtil.generateToken("admin", "ADMIN");

        // Different 256-bit key
        byte[] otherKeyBytes = new byte[32];
        otherKeyBytes[0] = 1;
        String otherSecret = Base64.getEncoder().encodeToString(otherKeyBytes);
        JwtUtil otherJwtUtil = new JwtUtil(otherSecret, EXPIRATION_MS);

        assertThat(otherJwtUtil.isValid(token)).isFalse();
    }

    @Test
    void getUsername_returnsCorrectSubject() {
        String token = jwtUtil.generateToken("testuser", "VIEWER");

        assertThat(jwtUtil.getUsername(token)).isEqualTo("testuser");
    }

    @Test
    void getRole_returnsCorrectRole() {
        String token = jwtUtil.generateToken("admin", "ADMIN");

        assertThat(jwtUtil.getRole(token)).isEqualTo("ADMIN");
    }

    @Test
    void generateToken_differentUsersProduceDifferentTokens() {
        String token1 = jwtUtil.generateToken("user1", "VIEWER");
        String token2 = jwtUtil.generateToken("user2", "ADMIN");

        assertThat(token1).isNotEqualTo(token2);
    }
}