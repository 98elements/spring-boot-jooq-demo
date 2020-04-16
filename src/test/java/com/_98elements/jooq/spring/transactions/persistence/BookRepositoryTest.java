package com._98elements.jooq.spring.transactions.persistence;

import com._98elements.jooq.spring.transactions.persistance.BookRepository;
import com._98elements.jooq.spring.transactions.persistence.public_.tables.records.BooksRecord;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static com._98elements.jooq.spring.transactions.persistence.public_.tables.Books.BOOKS;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class BookRepositoryTest {

  @Autowired
  private DSLContext dslContext;

  @Autowired
  private BookRepository bookRepository;

  @BeforeEach
  void cleanUp() {
    dslContext.deleteFrom(BOOKS).execute();
  }

  @Test
  void thatBookCanBeInserted() {
    //when
    bookRepository.insert(1, "some title");

    //then
    assertThat(bookRepository.getBooks()).containsExactly(new BooksRecord(1, "some title"));
  }

  @Test
  void thatBooksCanBeFetched() {
    //given
    bookRepository.insert(1, "some title");
    bookRepository.insert(2, "some other title");

    //when
    final Result<BooksRecord> books = bookRepository.getBooks();

    //then
    assertThat(books)
      .containsExactly(
        new BooksRecord(1, "some title"),
        new BooksRecord(2, "some other title")
      );
  }

}
