package com._98elements.jooq.spring.transactions;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.IllegalTransactionStateException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JooqSpringTransactionMandatoryTest extends JooqSpringTransactionDemoTest {

  @Test
  void thatTransactionIsMandatory() {
    assertThatThrownBy(
      () -> transactionalRunner.doInTransactionMandatory(() -> {}) // will throw Exception
    ).isInstanceOf(IllegalTransactionStateException.class);
  }
}
