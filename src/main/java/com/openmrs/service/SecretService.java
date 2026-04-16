package com.openmrs.service;

import com.openmrs.model.Secret;
import com.openmrs.repository.SecretRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Service
public class SecretService {

    private static final String AES_GCM = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 128;
    private static final Pattern SECRET_REF_PATTERN = Pattern.compile("\\$\\{\\{\\s*secrets\\.([\\w.-]+)\\s*}}");

    private final SecretRepository secretRepository;
    private final SecretKeySpec keySpec;

    public SecretService(SecretRepository secretRepository,
                         @Value("${secrets.encryption-key}") String encryptionKey) {
        this.secretRepository = secretRepository;
        byte[] keyBytes = Base64.getDecoder().decode(encryptionKey);
        this.keySpec = new SecretKeySpec(keyBytes, "AES");
    }

    @Transactional
    public Secret createOrUpdate(String name, String plainValue) {
        Secret secret = secretRepository.findByName(name).orElse(new Secret());
        secret.setName(name);
        secret.setEncryptedValue(encrypt(plainValue));
        return secretRepository.save(secret);
    }

    @Transactional(readOnly = true)
    public List<String> listSecretNames() {
        return secretRepository.findAll().stream()
                .map(Secret::getName)
                .toList();
    }

    @Transactional
    public void delete(String name) {
        if (!secretRepository.existsByName(name)) {
            throw new IllegalArgumentException("Secret not found: " + name);
        }
        secretRepository.deleteByName(name);
    }

    /**
     * Resolves all ${{ secrets.NAME }} references in a string to their decrypted values.
     * Returns the original string if it contains no secret references.
     */
    public String resolveSecrets(String value) {
        if (value == null || !value.contains("${{")) {
            return value;
        }

        Matcher matcher = SECRET_REF_PATTERN.matcher(value);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            String secretName = matcher.group(1);
            Secret secret = secretRepository.findByName(secretName)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Secret '" + secretName + "' not found. Create it via POST /api/secrets first."));
            matcher.appendReplacement(result, Matcher.quoteReplacement(decrypt(secret.getEncryptedValue())));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    /**
     * Checks if a string contains secret references.
     */
    public boolean containsSecretRef(String value) {
        return value != null && SECRET_REF_PATTERN.matcher(value).find();
    }

    private String encrypt(String plainText) {
        try {
            byte[] iv = new byte[GCM_IV_LENGTH];
            new SecureRandom().nextBytes(iv);

            Cipher cipher = Cipher.getInstance(AES_GCM);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, new GCMParameterSpec(GCM_TAG_LENGTH, iv));
            byte[] encrypted = cipher.doFinal(plainText.getBytes());

            // Prepend IV to ciphertext
            ByteBuffer buffer = ByteBuffer.allocate(iv.length + encrypted.length);
            buffer.put(iv);
            buffer.put(encrypted);

            return Base64.getEncoder().encodeToString(buffer.array());
        } catch (Exception e) {
            throw new RuntimeException("Failed to encrypt secret", e);
        }
    }

    private String decrypt(String encryptedText) {
        try {
            byte[] decoded = Base64.getDecoder().decode(encryptedText);
            ByteBuffer buffer = ByteBuffer.wrap(decoded);

            byte[] iv = new byte[GCM_IV_LENGTH];
            buffer.get(iv);
            byte[] cipherText = new byte[buffer.remaining()];
            buffer.get(cipherText);

            Cipher cipher = Cipher.getInstance(AES_GCM);
            cipher.init(Cipher.DECRYPT_MODE, keySpec, new GCMParameterSpec(GCM_TAG_LENGTH, iv));

            return new String(cipher.doFinal(cipherText));
        } catch (Exception e) {
            throw new RuntimeException("Failed to decrypt secret", e);
        }
    }
}