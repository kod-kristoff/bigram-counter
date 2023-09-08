use rusqlite::{params, Connection};
use std::io::BufRead;
use std::io::Write;
use std::time::Instant;
use std::{
    env, fs, io,
    path::{Path, PathBuf},
};

fn main() {
    let start = Instant::now();
    if let Err(err) = try_main() {
        eprintln!("Error: {:#?}", err);
        std::process::exit(1);
    }
    let elapsed = start.elapsed();
    eprintln!("Running time: {:?}", elapsed);
}

fn try_main() -> eyre::Result<()> {
    let mut args = env::args().skip(1);
    let dir: PathBuf = args.next().map(|s| s.into()).expect("'DIR' is required");
    let output: PathBuf = args
        .next()
        .map(|s| s.into())
        .expect("'OUTPUT-SLUG' is required");
    let conn = Connection::open("./bigrams.db3")?;
    setup_db(&conn)?;
    walk_dir(&dir, &conn)?;
    dump_freqs(&output, &conn)?;
    Ok(())
}

fn setup_db(conn: &Connection) -> eyre::Result<()> {
    conn.execute("DROP TABLE IF EXISTS freqs", ())?;
    conn.execute(
        "CREATE TABLE freqs (
            word STRING PRIMARY KEY,
            count INTEGER
        )",
        (),
    )?;
    Ok(())
}
fn walk_dir(dir: &Path, conn: &Connection) -> eyre::Result<()> {
    eprintln!("reading dir {} ...", dir.display());
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        let meta = fs::metadata(&path)?;

        if meta.is_dir() {
            walk_dir(&path, conn)?;
            continue;
        }
        if meta.is_file() {
            if let Some(file_stem) = path.as_path().file_stem() {
                if file_stem.to_string_lossy() == ".DS_Store" {
                    continue;
                }
            }
            process_file(&path, conn)?;
            continue;
        }
    }
    Ok(())
}

fn process_file(path: &Path, conn: &Connection) -> eyre::Result<()> {
    eprintln!("processing file {} ...", path.display());
    let mut select_stmt = conn.prepare_cached("SELECT count FROM freqs WHERE word = ?1")?;
    let mut insert_word =
        conn.prepare_cached("INSERT OR IGNORE INTO freqs (word, count) VALUES (?1, ?2)")?;
    let mut update_word_count =
        conn.prepare_cached("UPDATE freqs SET count = ?2 WHERE word = ?1")?;
    let file = fs::File::open(path)?;
    let reader = io::BufReader::new(file);
    let mut total_file_count = 0;
    for (i, line) in reader.lines().enumerate() {
        let line = line?;
        if i < 5 {
            eprintln!("skipping line: '{}'", line);
            if !line.starts_with("@") {
                panic!("unexpected line")
            }
            continue;
        }
        let mut parts = line.split_whitespace();
        let word: &str = parts.next().expect("a string");
        let count: usize = parts.next().expect("a string").parse()?;
        total_file_count += count;
        let accu_count: usize = match select_stmt.query([word]) {
            Ok(mut count_rows) => {
                if let Some(count_row) = count_rows.next()? {
                    count_row.get(0)?
                } else {
                    // eprintln!("found no row for '{}'", word);
                    0
                }
            }
            Err(err) => {
                eprintln!("ignoring '{}'", err);
                0
            }
        };

        let accu_count = accu_count + count;
        update_word_count.execute(params![word, accu_count])?;
        insert_word.execute(params![word, accu_count])?;
    }
    eprintln!(
        "Total count for file '{}': {}",
        path.display(),
        total_file_count
    );
    Ok(())
}

fn dump_freqs(output: &Path, conn: &Connection) -> eyre::Result<()> {
    let mut output_freqs = PathBuf::from(output);
    output_freqs.set_extension("freqs.txt");
    let mut output_bigrams = PathBuf::from(output);
    output_bigrams.set_extension("bigrams.txt");

    eprintln!(
        "writing bigrams sorted descending by count to '{}' ...",
        output_freqs.display()
    );
    let file_freqs = fs::File::create(&output_freqs)?;
    let mut writer_freqs = io::BufWriter::new(file_freqs);

    eprintln!(
        "writing bigrams sorted ascending by bigram to '{}' ...",
        output_bigrams.display()
    );
    let file_bigrams = fs::File::create(&output_bigrams)?;
    let mut writer_bigrams = io::BufWriter::new(file_bigrams);

    let mut stmt = conn.prepare("SELECT SUM(count) FROM freqs")?;
    if let Some(row) = stmt.query([])?.next()? {
        let size: usize = row.get(0)?;
        writeln!(writer_freqs, "@size\t{}", size)?;
        writeln!(writer_bigrams, "@size\t{}", size)?;
    }
    let mut stmt = conn.prepare("SELECT SUM(count) FROM freqs WHERE count>1")?;
    if let Some(row) = stmt.query([])?.next()? {
        let size_1: usize = row.get(0)?;
        writeln!(writer_freqs, "@size-1\t{}", size_1)?;
        writeln!(writer_bigrams, "@size-1\t{}", size_1)?;
    }
    let mut select_all_words =
        conn.prepare("SELECT word, count FROM freqs WHERE count>1 ORDER BY count DESC")?;
    let mut rows = select_all_words.query([])?;
    let mut rows_written = 0;
    while let Some(row) = rows.next()? {
        let word: String = row.get(0)?;
        let count: usize = row.get(1)?;
        writeln!(writer_freqs, "{}\t{}", word, count)?;
        rows_written += 1;
    }
    eprintln!(
        "wrote {} rows to '{}'",
        rows_written,
        output_freqs.display()
    );
    let mut select_all_words =
        conn.prepare("SELECT word, count FROM freqs WHERE count>1 ORDER BY word ASC")?;
    let mut rows = select_all_words.query([])?;
    let mut rows_written = 0;
    while let Some(row) = rows.next()? {
        let word: String = row.get(0)?;
        let count: usize = row.get(1)?;
        writeln!(writer_bigrams, "{}\t{}", word, count)?;
        rows_written += 1;
    }
    eprintln!(
        "wrote {} rows to '{}'",
        rows_written,
        output_bigrams.display()
    );
    Ok(())
}
