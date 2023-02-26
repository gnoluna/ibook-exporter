use std::path::{Path, PathBuf};

use glob::{glob, glob_with, MatchOptions};
use rusqlite::{Connection, Result, Rows, Statement};

#[derive(Debug)]
pub struct Table<'a> {
    tablename: &'a str,
    fieldname_mappings: &'a [(&'a str, &'a str)],
    db_pattern: &'a str,
}

#[derive(Debug)]
pub struct Annotation {
    asset_id: String,
    selected_text: Option<String>,
}

#[derive(Debug)]
pub struct Book {
    asset_id: String,
    author: String,
    title: String,
}

pub const BOOK_INFO_TABLE: Table = Table {
    tablename: "ZBKLIBRARYASSET",
    fieldname_mappings: &[
        ("ZTITLE", "title"),
        ("ZAUTHOR", "author"),
        ("ZASSETID", "asset_id"),
    ],
    db_pattern: "BKLibrary/BKLibrary*.sqlite",
};

pub const ANNOTATION_TABLE: Table = Table {
    tablename: "ZAEANNOTATION",
    fieldname_mappings: &[
        ("ZANNOTATIONASSETID", "asset_id"),
        ("ZANNOTATIONSELECTEDTEXT", "selected_text"),
        ("ZANNOTATIONNOTE", "note"),
        ("ZANNOTATIONREPRESENTATIVETEXT", "represent_text"),
        ("ZFUTUREPROOFING5", "chapter"),
        ("ZANNOTATIONSTYLE", "stype"),
        ("ZANNOTATIONMODIFICATIONDATE", "modified_date"),
        ("ZANNOTATIONLOCATION", "location"),
    ],
    db_pattern: "AEAnnotation/AEAnnotation*.sqlite",
};

pub trait Selectable {
    fn selected_fields(&self) -> String;
    fn conn(&self) -> Result<Connection>;
    fn select_all_stmt(&self) -> String;
    fn db(&self) -> String;
}

impl Selectable for Table<'_> {
    fn selected_fields(&self) -> String {
        self.fieldname_mappings
            .iter()
            .map(|(column, alias)| format!("{} as {}", column, alias))
            .collect::<Vec<_>>()
            .join(",")
    }
    fn conn(&self) -> Result<Connection> {
        Ok(Connection::open(self.db())?)
    }
    fn select_all_stmt(&self) -> String {
        format!(
            "SELECT {selected_fields} FROM {table_name}",
            table_name = self.tablename,
            selected_fields = self.selected_fields()
        )
    }
    fn db(&self) -> String {
        let mut home_dir = dirs::home_dir().expect("Home dir not found");
        let pattern = Path::new(&home_dir)
            .join("Library/Containers/com.apple.iBooksX/Data/Documents")
            .join(self.db_pattern);
        let pattern = pattern.to_string_lossy();

        let db = glob(&pattern).expect("").next().and_then(|path| {
            match path.unwrap().into_os_string().into_string() {
                Ok(res) => Some(res),
                Err(e) => {
                    println!("{:?}", e);
                    None
                }
            }
        });
        db.unwrap()
    }
}

pub fn select_all_tables() -> Result<Vec<Book>> {
    BOOK_INFO_TABLE
        .conn()?
        .prepare(BOOK_INFO_TABLE.select_all_stmt().as_str())?
        .query_map([], |row| {
            Ok(Book {
                title: row.get(0)?,
                author: row.get(1)?,
                asset_id: row.get(2)?,
            })
        })?
        .collect()
}

pub fn select_all_highlights_for_book(book: &Book) -> Result<Vec<Annotation>> {
    let select_all_annotations = ANNOTATION_TABLE.select_all_stmt();
    let stmt = &format!(
        "{} WHERE asset_id = {}",
        select_all_annotations, book.asset_id
    );
    ANNOTATION_TABLE
        .conn()?
        .prepare(&stmt)?
        .query_map([], |row| {
            Ok(Annotation {
                asset_id: row.get(0)?,
                selected_text: row.get(1)?,
            })
        })?
        .collect()
}
