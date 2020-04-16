package com._98elements.jooq.spring.transactions;

import com._98elements.jooq.spring.transactions.persistance.BookRepository;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static com._98elements.jooq.spring.transactions.persistence.public_.tables.Books.BOOKS;

@SpringBootTest
abstract class JooqSpringTransactionDemoTest {

  @Autowired
  private DSLContext dslContext;

  @Autowired
  protected BookRepository bookRepository;

  @Autowired
  protected TransactionalRunner transactionalRunner;

  @BeforeEach
  void cleanUp() {
    dslContext.deleteFrom(BOOKS).execute();
  }

}
