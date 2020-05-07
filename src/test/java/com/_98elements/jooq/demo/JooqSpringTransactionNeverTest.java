package com._98elements.jooq.demo;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.IllegalTransactionStateException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JooqSpringTransactionNeverTest extends JooqSpringTransactionTest {

  @Test
  void thatTransactionIsNotCreated() {
    assertThat(bookRepository.getBooks().size()).isZero();

    assertThatThrownBy(
      () -> transactionalRunner.doInTransactionNever(
        () -> {
          bookRepository.insert(someBook(1));
          bookRepository.insert(someBook(1)); // will throw Exception
        })
    ).isInstanceOf(DataAccessException.class);

    assertThat(bookRepository.getBooks().size()).isOne();
  }

  @Test
  void thatNestedExecutionsCannotBeRunInTransaction() {
    assertThatThrownBy(
      () -> transactionalRunner.doInTransaction(
        () -> transactionalRunner.doInTransactionNever(() -> {}) // will throw Exception
      )
    ).isInstanceOf(IllegalTransactionStateException.class);
  }
}
