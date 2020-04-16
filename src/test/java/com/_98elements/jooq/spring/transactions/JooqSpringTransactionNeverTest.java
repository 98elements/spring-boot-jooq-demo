package com._98elements.jooq.spring.transactions;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.IllegalTransactionStateException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JooqSpringTransactionNeverTest extends JooqSpringTransactionDemoTest {

  @Test
  void thatTransactionIsNotCreated() {
    assertThat(bookRepository.getBooks().size()).isZero();

    assertThatThrownBy(
      () -> transactionalRunner.doInTransactionNever(
        () -> {
          bookRepository.insert(1, "some tittle");
          bookRepository.insert(1, "same tittle"); // will throw Exception
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
