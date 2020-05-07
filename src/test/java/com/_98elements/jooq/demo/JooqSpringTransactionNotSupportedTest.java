package com._98elements.jooq.demo;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JooqSpringTransactionNotSupportedTest extends JooqSpringTransactionTest {

  @Test
  void thatTransactionIsNotCreated() {
    assertThat(bookRepository.getBooks().size()).isZero();

    assertThatThrownBy(
      () -> transactionalRunner.doInTransactionNotSupported(
        () -> {
          bookRepository.insert(someBook(1));
          bookRepository.insert(someBook(1)); // will throw Exception
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
          bookRepository.insert(someBook(1));
          transactionalRunner.doInTransactionNotSupported(
            () -> bookRepository.insert(someBook(2))
          );
          bookRepository.insert(someBook(2)); // will throw Exception
        })
    ).isInstanceOf(DataAccessException.class);

    assertThat(bookRepository.getBooks().size()).isOne();
    //assertThat(bookRepository.getBooks()).containsExactly(new BooksRecord(2, "si-fi", "some tittle"));
  }
}
