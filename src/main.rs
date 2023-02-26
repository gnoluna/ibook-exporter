use crate::models::{select_all_highlights_for_book, select_all_tables};

pub mod models;

fn main() {
    println!("Hello, world!");
    let books = select_all_tables().expect("Failed to get books");
    println!("{:?}", books);
    let annotations = select_all_highlights_for_book(books.get(10).expect(""));
    println!("{:?}", annotations);
}
