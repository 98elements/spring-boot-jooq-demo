package com._98elements.jooq.demo.persistence;

import com._98elements.jooq.demo.persistence.BookRepository.BookGenreRank;
import com._98elements.jooq.demo.persistence.BookRepository.GenreAuthorAvgPrice;
import com._98elements.jooq.demo.persistence.public_.tables.records.BooksRecord;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com._98elements.jooq.demo.persistence.public_.tables.Books.BOOKS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
class BookRepositoryTest {  private final static String DATA = """
        1	Fantasy	'Goddess Of The Land'	Serena Calderon	200
        2	Romance	'Source With Honor'	Serena Calderon	140
        3	Thriller	'Turtle Without Sin'	Harriett Porter	80
        4	Fantasy	'Foreigners Of The Lost Ones'	Eoin Holland	140
        5	Romance	'Boys Of Dread'	Serena Calderon	80
        6	Thriller	'Fish And Turtles'	Harriett Porter	100
        7	Fantasy	'Restoration Of The Gods'	Harriett Porter	240
        8	Romance	'Changing My Wife'	Serena Calderon	140
        9	Fantasy	'Avoiding The City'	Eoin Holland	320
        """;

  private final static List<GenreAuthorAvgPrice> avgGenreAuthorPrices = List.of(
    new GenreAuthorAvgPrice("Fantasy", "Serena Calderon", 200),
    new GenreAuthorAvgPrice("Romance", "Serena Calderon", 120),
    new GenreAuthorAvgPrice("Fantasy", "Eoin Holland", 230),
    new GenreAuthorAvgPrice("Thriller", "Harriett Porter", 90),
    new GenreAuthorAvgPrice("Fantasy", "Harriett Porter", 240)
  );

  private final static List<GenreAuthorAvgPrice> avgGenrePrices = List.of(
    new GenreAuthorAvgPrice("Fantasy", null, 225),
    new GenreAuthorAvgPrice("Romance", null, 120),
    new GenreAuthorAvgPrice("Thriller", null, 90)
  );

  private final static List<GenreAuthorAvgPrice> avgAuthorPrices = List.of(
    new GenreAuthorAvgPrice(null, "Serena Calderon", 140),
    new GenreAuthorAvgPrice(null, "Eoin Holland", 230),
    new GenreAuthorAvgPrice(null, "Harriett Porter", 140)
  );

  private final static GenreAuthorAvgPrice avgPrice =
    new GenreAuthorAvgPrice(null, null, 160);

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
    //given
    final var book = someBook(1);

    //when
    bookRepository.insert(book);

    //then
    assertThat(bookRepository.getBooks()).containsExactly(book);
  }

  @Test
  void thatBookCanBeInsertedInTransaction() {
    //given
    final var someBook = someBook(1);
    final var otherBook = someBook(2);

    //when
    bookRepository.insertAll(List.of(someBook, otherBook));

    //then
    assertThat(bookRepository.getBooks()).containsExactlyInAnyOrder(someBook, otherBook);
  }

  @Test
  void thatSameBookCannotBeInsertedInTransaction() {
    //given
    final var someBook = someBook(1);
    final var sameBook = someBook.copy();

    //expect
    assertThatThrownBy(() -> bookRepository.insertAll(List.of(someBook, sameBook)))
      .isInstanceOf(DataAccessException.class);
  }

  @Test
  void thatBooksCanBeFetched() {
    //given
    final var book1 = someBook(1);
    final var book2 = someBook(2);

    bookRepository.insert(book1);
    bookRepository.insert(book2);

    //when
    final Result<BooksRecord> books = bookRepository.getBooks();

    //then
    assertThat(books).containsExactlyInAnyOrder(book1, book2);
  }

  @Test
  void thatRowsAreInserted() throws IOException {
    bookRepository.loadCsv(DATA);

    assertThat(bookRepository.getBooks()).hasSize(9);
  }

  @Test
  void thatRowsCanBeFetchedAsCSV() throws IOException {
    bookRepository.loadCsv(DATA);

    assertThat(bookRepository.getCSV()).isEqualTo(DATA);
  }

  @Test
  void thatRowsCanBeGrouped() throws IOException {
    bookRepository.loadCsv(DATA);

    assertThat(bookRepository.streamGroupingAvgPriceByGenre())
      .containsExactlyInAnyOrderElementsOf(bookRepository.groupAvgPriceByGenre());

    assertThat(bookRepository.parallelStreamGroupingAvgPriceByGenre())
      .containsExactlyInAnyOrderElementsOf(bookRepository.groupAvgPriceByGenre());
  }

  @Test
  void thatCubeGroupingIsSupported() throws IOException {
    bookRepository.loadCsv(DATA);

    final List<GenreAuthorAvgPrice> result = bookRepository.cubeAvgPriceByGenreAndAuthor();

    final var expectedResults = new ArrayList<GenreAuthorAvgPrice>();
    expectedResults.addAll(avgGenreAuthorPrices);
    expectedResults.addAll(avgAuthorPrices);
    expectedResults.addAll(avgGenrePrices);
    expectedResults.add(avgPrice);

    assertThat(result).containsExactlyInAnyOrderElementsOf(expectedResults);
  }

  @Test
  void thatRollupGroupingIsSupported() throws IOException {
    bookRepository.loadCsv(DATA);

    final List<GenreAuthorAvgPrice> result = bookRepository.rollupAvgPriceByGenreAndAuthor();

    final var expectedResults = new ArrayList<GenreAuthorAvgPrice>();
    expectedResults.addAll(avgGenreAuthorPrices);
    expectedResults.addAll(avgGenrePrices);
    expectedResults.add(avgPrice);

    assertThat(result).containsExactlyInAnyOrderElementsOf(expectedResults);
  }

  @Test
  void thatKaysetPaginationIsSupported() throws IOException {
    bookRepository.loadCsv(DATA);

    final List<BooksRecord> page1 = bookRepository.getPageByKeys(0, 0);
    assertThat(page1).extracting(BooksRecord::getId).containsExactly(3, 5, 6, 2);

    final BooksRecord lastRecord = page1.get(3);

    final List<BooksRecord> page2 = bookRepository.getPageByKeys(lastRecord.getPrice(), lastRecord.getId());
    assertThat(page2).extracting(BooksRecord::getId).containsExactly(4, 8, 1, 7);
  }

  @Test
  void thatWithClauseIsSupported() throws IOException {
    bookRepository.loadCsv(DATA);

    final List<BooksRecord> result = bookRepository.getBooksWithPriceGreaterThanGenreAverage();

    assertThat(result).extracting(BooksRecord::getId).containsExactlyInAnyOrder(2, 6, 7, 8, 9);
  }

  @Test
  void thatPartitionWindowAggregationsAreSupported() throws IOException {
    bookRepository.loadCsv(DATA);

    final List<BookGenreRank> result = bookRepository.getBooksGenreRank();

    assertThat(result).containsExactly(
      new BookGenreRank(1, "Fantasy", "'Goddess Of The Land'", 1),
      new BookGenreRank(2, "Romance", "'Source With Honor'", 1),
      new BookGenreRank(3, "Thriller", "'Turtle Without Sin'", 1),
      new BookGenreRank(4, "Fantasy", "'Foreigners Of The Lost Ones'", 2),
      new BookGenreRank(5, "Romance", "'Boys Of Dread'", 2),
      new BookGenreRank(6, "Thriller", "'Fish And Turtles'", 2),
      new BookGenreRank(7, "Fantasy", "'Restoration Of The Gods'", 3),
      new BookGenreRank(8, "Romance", "'Changing My Wife'", 3),
      new BookGenreRank(9, "Fantasy", "'Avoiding The City'", 4)
    );
  }

  private static BooksRecord someBook(final Integer id) {
    return new BooksRecord(id, "random genre", "Random title", "Sam Some", 120);
  }
}
