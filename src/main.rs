use rusqlite::Connection;
use std::io::BufRead;
use std::{
    env, fs, io,
    path::{Path, PathBuf},
};

fn main() {
    if let Err(err) = try_main() {
        eprintln!("Error: {:#?}", err);
        std::process::exit(1);
    }
}

fn try_main() -> eyre::Result<()> {
    let mut args = env::args().skip(1);
    let dir: PathBuf = args
        .next()
        .map(|s| s.into())
        .unwrap_or_else(|| PathBuf::from("2gram"));
    let conn = Connection::open("./bigrams.db3")?;
    setup_db(&conn)?;
    walk_dir(&dir, &conn)?;
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
            continue
            ;
        }
        if meta.is_file() {
            process_file(&path, conn)?;
            continue;
        }
    }
    Ok(())
}

fn process_file(path: &Path, conn: &Connection) -> eyre::Result<()> {
    eprintln!("processing file {} ...", path.display());
    let mut select_stmt = conn.prepare_cached("SELECT count FROM freqs WHERE word = ?1")?;
    let mut insert_word = :wq
    let file = fs::File::open(path)?;
    let reader = io::BufReader::new(file);
    for (i, line) in reader.lines().enumerate() {
        let line = line?;
        if i < 5 {
            eprintln!("skipping line: '{}'", line);
            if !line.starts_with("@") {
                panic!("unexpected line")
            }
        }
        let mut parts = line.split_whitespace();
        let word: &str = parts.next().expect("a string");
        let count: usize = parts.next().expect("a string").parse()?;
        match select_stmt.execute([word]) {
            Ok(accu_count) => {}
        }
    }
    Ok(())
}
