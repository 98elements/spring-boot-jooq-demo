package com._98elements.jooq.spring.transactions;

import com._98elements.jooq.spring.transactions.persistence.public_.tables.records.BooksRecord;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JooqSpringTransactionNotSupportedTest extends JooqSpringTransactionDemoTest {

  @Test
  void thatTransactionIsNotCreated() {
    assertThat(bookRepository.getBooks().size()).isZero();

    assertThatThrownBy(
      () -> transactionalRunner.doInTransactionNotSupported(
        () -> {
          bookRepository.insert(1, "some tittle");
          bookRepository.insert(1, "same tittle"); // will throw Exception
        })
    ).isInstanceOf(DataAccessException.class);

    assertThat(bookRepository.getBooks().size()).isOne();
  }

  @Test
  void thatNestedExecutionIsIndependent() {
    assertThat(bookRepository.getBooks().size()).isZero();

    assertThatThrownBy(
      () -> transactionalRunner.doInTransaction(
        () -> {
          bookRepository.insert(1, "some tittle");
          transactionalRunner.doInTransactionNotSupported(
            () -> bookRepository.insert(2, "some tittle")
          );
          bookRepository.insert(2, "same tittle"); // will throw Exception
        })
    ).isInstanceOf(DataAccessException.class);

    assertThat(bookRepository.getBooks().size()).isOne();
    assertThat(bookRepository.getBooks()).containsExactly(new BooksRecord(2, "some tittle"));
  }
}
