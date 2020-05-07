package com._98elements.jooq.demo;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.IllegalTransactionStateException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JooqSpringTransactionMandatoryTest extends JooqSpringTransactionTest {

  @Test
  void thatTransactionIsMandatory() {
    assertThatThrownBy(
      () -> transactionalRunner.doInTransactionMandatory(() -> {}) // will throw Exception
    ).isInstanceOf(IllegalTransactionStateException.class);
  }
}
