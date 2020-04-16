package com._98elements.jooq.spring.transactions;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionalRunner {

  @Transactional(propagation = Propagation.REQUIRED)
  public void doInTransaction(final Runnable runnable) { runnable.run(); }

  @Transactional(propagation = Propagation.SUPPORTS)
  public void doInTransactionSupports(final Runnable runnable) { runnable.run(); }

  @Transactional(propagation = Propagation.MANDATORY)
  public void doInTransactionMandatory(final Runnable runnable) { runnable.run(); }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void doInTransactionRequiresNew(final Runnable runnable) { runnable.run(); }

  @Transactional(propagation = Propagation.NOT_SUPPORTED)
  public void doInTransactionNotSupported(final Runnable runnable) { runnable.run(); }

  @Transactional(propagation = Propagation.NEVER)
  public void doInTransactionNever(final Runnable runnable) { runnable.run(); }

  @Transactional(propagation = Propagation.NESTED)
  public void doInTransactionNested(final Runnable runnable) { runnable.run(); }
}
