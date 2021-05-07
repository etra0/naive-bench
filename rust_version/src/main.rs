use simd_json::{self, ValueAccess};
use std::fs;
use std::io::{Write, BufWriter};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use rayon::prelude::*;
use std::time;
use chrono::prelude::*;

type ResultBoxed<O> = std::result::Result<O, Box<dyn std::error::Error>>;
type ArcMutex<T> = Arc<Mutex<T>>;

#[derive(Debug)]
struct Entry {
    id: String,
    author_id: String,
    author_name: String,
    author_gender: Option<String>,
    timestamp: String,
    reactions: usize,
    url: String,
    comment: Option<String>,
}

fn format_option<'a>(input: &'a Option<String>) -> &'a str {
    match input {
        Some(s) => &s,
        None => "",
    }
}

impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{};{};{};{};{};{};{};{}",
            self.id,
            self.author_id,
            self.author_name,
            format_option(&self.author_gender),
            self.timestamp,
            self.reactions,
            self.url,
            format_option(&self.comment)
        )
    }
}

fn format_date(timestamp: u64) -> String {
    let date = Utc.timestamp(timestamp as _, 0);
    return date.to_string()
}

fn parse_json(f: &PathBuf, result: &ArcMutex<Vec<Entry>>) -> ResultBoxed<()> {
    let mut local_results: Vec<Entry> = vec![];
    let mut buf = std::fs::read(f)?;
    let parsed_json: simd_json::BorrowedValue = match simd_json::to_borrowed_value(&mut buf[..]) {
        Ok(s) => s,
        Err(_) => {
            println!("Skipping {:?}", f);
            return Ok(());
        }
    };

    let nodes = parsed_json["data"]["feedback"]["display_comments"]["edges"].as_array().unwrap();
    for n in nodes.iter() {
        let n = &n["node"];

        macro_rules! u {
            ($x:expr) => { $x.unwrap() }
        }

        local_results.push(Entry {
            id: u!(n["id"].as_str()).to_owned(),
            author_id: u!(n["author"]["id"].as_str()).to_owned(),
            author_name: u!(n["author"]["name"].as_str()).to_owned(),
            author_gender: n["author"].get_str("gender").map(|x| x.to_owned()),
            timestamp: format_date(u!(n["created_time"].as_u64()) as _),
            reactions: u!(n["feedback"]["reactors"]["count"].as_u64()) as _,
            url: u!(n["url"].as_str()).to_owned(),
            comment: n["body"].get_str("text").map(|x| x.to_owned())
        });
    }
    let mut end = result.lock().unwrap();
    end.extend(local_results);
    drop(end);

    Ok(())
}

fn multithreaded(data_path: &Vec<PathBuf>) -> ResultBoxed<()> {
    let results: ArcMutex<Vec<Entry>> =
        Arc::new(Mutex::new(Vec::with_capacity(2000 * 2000)));

    data_path
        .par_iter()
        .for_each(|path| {
            parse_json(path, &results).unwrap();
        });

    println!("Dumping the csv");
    let file = std::fs::File::create("output.csv")?;
    let mut buf = BufWriter::new(file);
    let mut vec = results.lock().unwrap();
    vec.par_sort_by(|a, b| a.id.partial_cmp(&b.id).unwrap());
    for n in vec.iter() {
        write!(buf, "{}\n", n)?;
    }

    Ok(())
}

fn main() -> ResultBoxed<()> {
    let args = std::env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        return Err("Not enough arguments".into());
    }

    let path = &args[1];

    let data_path: Vec<_> = fs::read_dir(path)?
        .filter_map(|x| x.ok())
        .map(|x| x.path())
        .collect();

    let now = time::Instant::now();
    multithreaded(&data_path)?;
    let result = time::Instant::now();
    println!("{} MB/s", 952. / result.duration_since(now).as_secs_f32());
    Ok(())
}
