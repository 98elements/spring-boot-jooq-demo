package com._98elements.jooq.demo;

import com._98elements.jooq.demo.persistence.BookRepository;
import com._98elements.jooq.demo.persistence.public_.tables.records.BooksRecord;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static com._98elements.jooq.demo.persistence.public_.tables.Books.BOOKS;

@SpringBootTest
abstract class JooqSpringTransactionTest {

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

  static BooksRecord someBook(final Integer id) {
    return new BooksRecord(id, "random genre", "Random title", "Sam Some", 120);
  }
}
