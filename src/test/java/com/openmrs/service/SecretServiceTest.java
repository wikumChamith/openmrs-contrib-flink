package com.openmrs.service;

import com.openmrs.model.Secret;
import com.openmrs.repository.SecretRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SecretServiceTest {

    @Mock
    private SecretRepository secretRepository;

    private SecretService secretService;

    // Valid base64-encoded 256-bit AES key
    private static final String ENCRYPTION_KEY = "HdF3lssbjZz2Is7KXLs9mx9O6l+iW/tOFEaPF/4AGBI=";

    @BeforeEach
    void setUp() {
        secretService = new SecretService(secretRepository, ENCRYPTION_KEY);
    }

    @Test
    void createOrUpdate_newSecret_encrypts() {
        when(secretRepository.findByName("db-password")).thenReturn(Optional.empty());
        when(secretRepository.save(any(Secret.class))).thenAnswer(inv -> inv.getArgument(0));

        Secret result = secretService.createOrUpdate("db-password", "mySecret123");

        assertThat(result.getName()).isEqualTo("db-password");
        assertThat(result.getEncryptedValue()).isNotEqualTo("mySecret123");
        assertThat(result.getEncryptedValue()).isNotEmpty();
    }

    @Test
    void createOrUpdate_existingSecret_updates() {
        Secret existing = new Secret();
        existing.setId(1);
        existing.setName("db-password");
        existing.setEncryptedValue("old-encrypted");

        when(secretRepository.findByName("db-password")).thenReturn(Optional.of(existing));
        when(secretRepository.save(any(Secret.class))).thenAnswer(inv -> inv.getArgument(0));

        Secret result = secretService.createOrUpdate("db-password", "newSecret");

        assertThat(result.getId()).isEqualTo(1);
        assertThat(result.getEncryptedValue()).isNotEqualTo("old-encrypted");
    }

    @Test
    void encryptDecrypt_roundTrip() {
        // Use createOrUpdate to encrypt, then simulate resolveSecrets to decrypt
        ArgumentCaptor<Secret> captor = ArgumentCaptor.forClass(Secret.class);
        when(secretRepository.findByName("test")).thenReturn(Optional.empty());
        when(secretRepository.save(captor.capture())).thenAnswer(inv -> inv.getArgument(0));

        secretService.createOrUpdate("test", "plain-text-value");

        String encrypted = captor.getValue().getEncryptedValue();

        // Now set up for resolveSecrets
        Secret secret = new Secret();
        secret.setName("test");
        secret.setEncryptedValue(encrypted);
        when(secretRepository.findByName("test")).thenReturn(Optional.of(secret));

        String resolved = secretService.resolveSecrets("${{ secrets.test }}");
        assertThat(resolved).isEqualTo("plain-text-value");
    }

    @Test
    void resolveSecrets_noReferences_returnsOriginal() {
        String input = "jdbc:mysql://localhost:3306/openmrs";
        String result = secretService.resolveSecrets(input);
        assertThat(result).isEqualTo(input);
    }

    @Test
    void resolveSecrets_nullInput_returnsNull() {
        assertThat(secretService.resolveSecrets(null)).isNull();
    }

    @Test
    void resolveSecrets_missingSecret_throws() {
        when(secretRepository.findByName("nonexistent")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> secretService.resolveSecrets("${{ secrets.nonexistent }}"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("nonexistent");
    }

    @Test
    void resolveSecrets_multipleReferences() {
        Secret userSecret = new Secret();
        userSecret.setName("db-user");
        Secret passSecret = new Secret();
        passSecret.setName("db-pass");

        // Encrypt values for the test
        when(secretRepository.findByName("temp")).thenReturn(Optional.empty());
        when(secretRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // Encrypt "admin"
        Secret s1 = secretService.createOrUpdate("temp", "admin");
        userSecret.setEncryptedValue(s1.getEncryptedValue());

        // Encrypt "secret123"
        Secret s2 = secretService.createOrUpdate("temp", "secret123");
        passSecret.setEncryptedValue(s2.getEncryptedValue());

        when(secretRepository.findByName("db-user")).thenReturn(Optional.of(userSecret));
        when(secretRepository.findByName("db-pass")).thenReturn(Optional.of(passSecret));

        String result = secretService.resolveSecrets("user=${{ secrets.db-user }}&pass=${{ secrets.db-pass }}");

        assertThat(result).isEqualTo("user=admin&pass=secret123");
    }

    @Test
    void containsSecretRef_withRef_returnsTrue() {
        assertThat(secretService.containsSecretRef("${{ secrets.mykey }}")).isTrue();
    }

    @Test
    void containsSecretRef_withoutRef_returnsFalse() {
        assertThat(secretService.containsSecretRef("plaintext")).isFalse();
    }

    @Test
    void containsSecretRef_null_returnsFalse() {
        assertThat(secretService.containsSecretRef(null)).isFalse();
    }

    @Test
    void containsSecretRef_secretRefVariations() {
        assertThat(secretService.containsSecretRef("${{secrets.key}}")).isTrue();
        assertThat(secretService.containsSecretRef("${{  secrets.key  }}")).isTrue();
        assertThat(secretService.containsSecretRef("prefix${{ secrets.key }}suffix")).isTrue();
    }

    @Test
    void delete_existingSecret_succeeds() {
        when(secretRepository.existsByName("mykey")).thenReturn(true);

        secretService.delete("mykey");

        verify(secretRepository).deleteByName("mykey");
    }

    @Test
    void delete_nonexistentSecret_throws() {
        when(secretRepository.existsByName("missing")).thenReturn(false);

        assertThatThrownBy(() -> secretService.delete("missing"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Secret not found");
    }

    @Test
    void listSecretNames_returnsNames() {
        Secret s1 = new Secret();
        s1.setName("key1");
        Secret s2 = new Secret();
        s2.setName("key2");
        when(secretRepository.findAll()).thenReturn(java.util.List.of(s1, s2));

        assertThat(secretService.listSecretNames()).containsExactly("key1", "key2");
    }
}