package com._98elements.jooq.demo.persistence;

import com._98elements.jooq.demo.persistence.public_.tables.records.BooksRecord;
import org.jooq.CSVFormat;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import static com._98elements.jooq.demo.persistence.public_.tables.Books.BOOKS;
import static java.util.stream.Collectors.averagingInt;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.cube;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.rollup;
import static org.jooq.impl.DSL.select;

@Repository
public class BookRepository {

  private final DSLContext dslContext;

  public BookRepository(final DSLContext dslContext) {
    this.dslContext = dslContext;
  }

  public void insert(final BooksRecord record) {
    dslContext.attach(record);
    record.insert();
  }

  @Transactional
  public void insertAll(final List<BooksRecord> records) {
    records.forEach(r -> {
      dslContext.attach(r);
      r.insert();
    });
  }

  public void loadCsv(final String csvData) throws IOException {
    dslContext.loadInto(BOOKS)
      .onDuplicateKeyError()
      .onErrorAbort()
      .loadCSV(csvData)
      .fields(BOOKS.ID, BOOKS.GENRE, BOOKS.TITLE, BOOKS.AUTHOR, BOOKS.PRICE)
      .separator('\t')
      .ignoreRows(0)
      .execute();
  }

  public String getCSV() {
    return dslContext.fetch(BOOKS).formatCSV(new CSVFormat().delimiter('\t').quoteString("").header(false));
  }

  public List<GenreAvgPrice> streamGroupingAvgPriceByGenre() {
    try (var stream = dslContext.selectFrom(BOOKS).stream()) {
      return stream.collect(groupingBy(BooksRecord::getGenre, averagingInt(BooksRecord::getPrice)))
          .entrySet().stream()
          .map(entry -> new GenreAvgPrice(entry.getKey(), entry.getValue().intValue()))
          .collect(toList());
    }
  }

  public List<GenreAvgPrice> parallelStreamGroupingAvgPriceByGenre() {
    return dslContext.selectDistinct(BOOKS.GENRE).from(BOOKS).fetchInto(String.class)
      .stream()
      .parallel()
      .map(genre ->
        new GenreAvgPrice(
          genre,
          dslContext.select(avg(BOOKS.PRICE))
            .from(BOOKS)
            .where(BOOKS.GENRE.eq(genre))
            .fetchOne()
            .into(Integer.class)
        )
      )
      .collect(toList());
  }

  public List<GenreAvgPrice> groupAvgPriceByGenre() {
    return dslContext.select(BOOKS.GENRE, avg(BOOKS.PRICE))
        .from(BOOKS)
        .groupBy(BOOKS.GENRE)
        .fetchInto(GenreAvgPrice.class);
  }

  public List<GenreAuthorAvgPrice> cubeAvgPriceByGenreAndAuthor() {
    return dslContext.select(BOOKS.GENRE, BOOKS.AUTHOR, avg(BOOKS.PRICE))
        .from(BOOKS)
        .groupBy(cube(BOOKS.GENRE, BOOKS.AUTHOR))
        .fetchInto(GenreAuthorAvgPrice.class);
  }

  public List<GenreAuthorAvgPrice> rollupAvgPriceByGenreAndAuthor() {
    return dslContext.select(BOOKS.GENRE, BOOKS.AUTHOR, avg(BOOKS.PRICE))
        .from(BOOKS)
        .groupBy(rollup(BOOKS.GENRE, BOOKS.AUTHOR))
        .fetchInto(GenreAuthorAvgPrice.class);
  }

   public List<BooksRecord> getPageByKeys(final Integer price, final Integer id) {
    return dslContext.selectFrom(BOOKS)
      .orderBy(BOOKS.PRICE.asc(), BOOKS.ID.asc())
      .seek(price, id)
      .limit(4)
      .fetchInto(BooksRecord.class);
  }

  public List<BooksRecord> getBooksWithPriceGreaterThanGenreAverage() {
    final var avgTable =
      name("avg").fields("genre", "avgGenrePrice")
        .as(
          select(BOOKS.GENRE, avg(BOOKS.PRICE))
            .from(BOOKS)
            .groupBy(BOOKS.GENRE)
        );

    final var genreField = avgTable.field("genre", String.class);
    final var genreAvgField = avgTable.field("avgGenrePrice", BigDecimal.class);

    return dslContext
      .with(avgTable)
      .select(BOOKS.ID, BOOKS.GENRE, BOOKS.AUTHOR, BOOKS.PRICE, genreAvgField)
      .from(BOOKS)
      .join(avgTable).on(BOOKS.GENRE.eq(genreField))
      .where(genreAvgField.minus(BOOKS.PRICE).lt(BigDecimal.ZERO))
      .fetchInto(BooksRecord.class);
  }

  public List<BookGenreRank> getBooksGenreRank() {
    return dslContext.select(
      BOOKS.ID,
      BOOKS.GENRE,
      BOOKS.TITLE,
      count()
        .over()
        .partitionBy(BOOKS.GENRE)
        .orderBy(BOOKS.ID)
        .rowsBetweenUnboundedPreceding()
        .andCurrentRow()
    )
      .from(BOOKS)
      .orderBy(BOOKS.ID)
      .fetchInto(BookGenreRank.class);
  }

  public Result<BooksRecord> getBooks() {
    return dslContext.selectFrom(BOOKS).fetch();
  }

  public record GenreAvgPrice(String genre, Integer avgPrice) {}

  public record GenreAuthorAvgPrice(String genre, String author, Integer avgPrice) {}

  public record BookGenreRank(Integer id, String genre, String title, Integer genreRank) {}
}
