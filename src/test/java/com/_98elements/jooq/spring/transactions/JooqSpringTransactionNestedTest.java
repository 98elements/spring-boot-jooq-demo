package com._98elements.jooq.spring.transactions;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JooqSpringTransactionNestedTest extends JooqSpringTransactionDemoTest {

  @Test
  void thatNewTransactionIsCreatedWhenNoTransactionExists() {
    assertThat(bookRepository.getBooks().size()).isZero();

    assertThatThrownBy(
      () -> transactionalRunner.doInTransactionNested(
        () -> {
          bookRepository.insert(1, "some tittle");
          bookRepository.insert(1, "same tittle"); // will throw Exception
        })
    ).isInstanceOf(DataAccessException.class);

    assertThat(bookRepository.getBooks().size()).isZero();
  }

  @Test
  void thatNestedTransactionIsRolledBackOnException() {
    assertThat(bookRepository.getBooks().size()).isZero();

    assertThatThrownBy(
      () -> transactionalRunner.doInTransaction(
        () -> {
          bookRepository.insert(1, "some tittle");
          transactionalRunner.doInTransactionNested(
            () -> bookRepository.insert(2, "some tittle")
          );
          bookRepository.insert(2, "same tittle"); // will throw Exception
        })
    ).isInstanceOf(DataAccessException.class);

    assertThat(bookRepository.getBooks().size()).isZero();
  }
}
