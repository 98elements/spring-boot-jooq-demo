package com._98elements.jooq.spring.transactions.persistance;

import com._98elements.jooq.spring.transactions.persistence.public_.tables.records.BooksRecord;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.springframework.stereotype.Repository;

import static com._98elements.jooq.spring.transactions.persistence.public_.tables.Books.BOOKS;

@Repository
public class BookRepository {

  private final DSLContext dslContext;

  public BookRepository(final DSLContext dslContext) {
    this.dslContext = dslContext;
  }

  public void insert(final Integer id, final String title) {
    dslContext.insertInto(BOOKS).set(BOOKS.ID, id).set(BOOKS.TITLE, title).execute();
  }

  public Result<BooksRecord> getBooks() {
    return dslContext.selectFrom(BOOKS).fetch();
  }
}
