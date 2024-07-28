package com.anil.batch.partition.repository;

import com.anil.batch.partition.model.TicketEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public interface TicketRepository extends JpaRepository<TicketEntity, Long> {
    @Modifying
    @Transactional
    @Query("update TicketEntity t set t.state = :state where t.id = :id")
    void updateStatusById(Long id, String state);

    @Query("select t from TicketEntity t where t.id >= :from and t.id <= :to")
    Page<TicketEntity> findBetweenId(Long from, Long to, PageRequest pageRequest);
}
