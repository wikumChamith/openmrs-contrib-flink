package com.openmrs.security;

import com.openmrs.model.Role;
import com.openmrs.model.UserAccount;
import com.openmrs.repository.UserAccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Order(1)
@RequiredArgsConstructor
public class DataInitializer implements CommandLineRunner {

    private final UserAccountRepository userAccountRepository;
    private final PasswordEncoder passwordEncoder;

    @Value("${admin.default-username:admin}")
    private String defaultUsername;

    @Value("${admin.default-password:admin123}")
    private String defaultPassword;

    @Override
    public void run(String... args) {
        if (userAccountRepository.count() == 0) {
            UserAccount admin = UserAccount.builder()
                    .username(defaultUsername)
                    .password(passwordEncoder.encode(defaultPassword))
                    .role(Role.ADMIN)
                    .build();
            userAccountRepository.save(admin);
            log.info("Default admin user '{}' created. Change the password immediately!", defaultUsername);
        }
    }
}