package com._98elements.jooq.demo;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JooqSpringTransactionNestedTest extends JooqSpringTransactionTest {

  @Test
  void thatNewTransactionIsCreatedWhenNoTransactionExists() {
    assertThat(bookRepository.getBooks().size()).isZero();

    assertThatThrownBy(
      () -> transactionalRunner.doInTransactionNested(
        () -> {
          bookRepository.insert(someBook(1));
          bookRepository.insert(someBook(1)); // will throw Exception
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
          bookRepository.insert(someBook(1));
          transactionalRunner.doInTransactionNested(
            () -> bookRepository.insert(someBook(2))
          );
          bookRepository.insert(someBook(2)); // will throw Exception
        })
    ).isInstanceOf(DataAccessException.class);

    assertThat(bookRepository.getBooks().size()).isZero();
  }
}
