package com.openmrs.repository;

import com.openmrs.model.Secret;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface SecretRepository extends JpaRepository<Secret, Integer> {
    Optional<Secret> findByName(String name);
    boolean existsByName(String name);
    void deleteByName(String name);
}