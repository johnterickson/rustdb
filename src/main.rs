#[macro_use]
extern crate array_macro;
#[macro_use]
extern crate static_assertions;

use std::fmt::{Display, Formatter};
use std::io::{BufRead, Read, Write};
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::process;
use std::str::{FromStr,from_utf8};

type Id = NonZeroUsize; // usize not good
type PageIndex = usize;

const PAGE_SIZE: usize = 4096;

#[repr(C)]
struct Row {
    id: Id,
    username: [u8; Row::USERNAME_SIZE],
    email: [u8; Row::EMAIL_SIZE],
}
const_assert!(size_of::<Row>() == size_of::<Id>() + Row::USERNAME_SIZE + Row::EMAIL_SIZE);

fn null_term_str(bytes: &[u8]) -> &str {
    let s = from_utf8(bytes).unwrap();
    s.split(0 as char).next().unwrap_or(s)
}

impl Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[{} '{}' '{}']",
            self.id,
            null_term_str(&self.username),
            null_term_str(&self.email)))
    }
}

impl Row {
    const USERNAME_SIZE: usize = 32;
    const EMAIL_SIZE: usize = 256;
    const SIZE: usize = size_of::<Row>();

    fn deserialize<R:Read>(r: &mut R) -> std::io::Result<Row> {
        let mut id = [0u8; size_of::<usize>()];
        r.read_exact(&mut id)?;
        let id = usize::from_le_bytes(id);
        let mut row = Row {
            id: NonZeroUsize::new(id).unwrap(),
            username: [0u8; Row::USERNAME_SIZE],
            email: [0u8; Row::EMAIL_SIZE],
        };
        r.read_exact(&mut row.username)?;
        r.read_exact(&mut row.email)?;
        Ok(row)
    }

    fn serialize<W:Write>(&self, w: &mut W) -> std::io::Result<()> {
        w.write(&self.id.get().to_le_bytes())?;
        w.write(&self.username)?;
        w.write(&self.email)?;
        Ok(())
    }
}

#[repr(C)]
struct Cell {
    key: Id,
    row: Row,
}

const_assert!(size_of::<Cell>() == size_of::<Id>() + size_of::<Row>());

impl Cell {
    const KEY_SIZE: usize = size_of::<Id>();
    const KEY_OFFSET: usize = 0;
    const VALUE_SIZE: usize = Row::SIZE;
    const VALUE_OFFSET: usize = Cell::KEY_OFFSET + Cell::KEY_SIZE;
    const SIZE: usize = Cell::VALUE_OFFSET + Cell::VALUE_SIZE;
}

struct Pager {

}

impl Pager {
    // fn get_mut(&mut self) -> &mut Page {

    // }
}

struct Cursor<'a> {
    pager: &'a Pager,
    page_index: PageIndex,
}

struct InternalNode {}

struct LeafNode {
    cells: [Option<Cell>; LeafNode::MAX_CELLS],
}

const_assert!(size_of::<LeafNode>() == LeafNode::MAX_CELLS * size_of::<Cell>());


impl LeafNode {
    const HEADER_SIZE: usize = size_of::<u32>();
    const SPACE_FOR_CELLS: usize = PAGE_SIZE - Page::HEADER_SIZE - LeafNode::HEADER_SIZE;
    const MAX_CELLS: usize = LeafNode::SPACE_FOR_CELLS / Cell::SIZE;
    const PADDING: usize = PAGE_SIZE - (LeafNode::MAX_CELLS * Cell::SIZE);

    fn create_empty() -> LeafNode {
        LeafNode {
            cells: array![None; LeafNode::MAX_CELLS]
        }
    }

    // fn insert(&mut self, cell:)
}

enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

struct Page {
    node: Node,
    parent: Option<PageIndex>,
}

impl Page {
    const HEADER_SIZE: usize = 6;
    const ROWS_PER_PAGE: usize = (PAGE_SIZE - Page::HEADER_SIZE) / Row::SIZE;

    fn create_leaf(parent: Option<PageIndex>) -> Page {
        Page {
            node: Node::Leaf(LeafNode::create_empty()),
            parent,
        }
    }

    fn serialize<W: Write>(&self, mut writer: W) {
        let mut buf = [0u8; PAGE_SIZE];
        buf[0] = match self.node {
            Node::Internal(_) => 0,
            Node::Leaf(_) => 1,
        };
        buf[1] = match self.parent {
            Some(_) => 0,
            None => 1,
        };
        buf[2..=5].copy_from_slice(&self.parent.unwrap_or(0).to_le_bytes());
        writer.write_all(&buf).unwrap();
    }
}

enum Statement {
    Insert(Id, String, String),
    Select
}

impl Statement {
    fn parse(line: &str) -> Option<Statement> {
        let mut tokens = line.split_whitespace();
        match tokens.next().unwrap() {
            "insert" => {
                let id = Id::from_str(tokens.next().expect("id not given")).expect("could not parse id");
                let username = tokens.next().expect("username not given").to_owned();
                let email = tokens.next().expect("email not given").to_owned();
                Some(Statement::Insert(id, username, email))
            },
            "select" => {
                Some(Statement::Select)
            }
            command => {
                eprintln!("Unknown command: {}", &command);
                None
            },
        }
    }
}

struct Table {
    num_rows: usize,
    pages: [Option<Box<Page>>; Table::MAX_PAGES],
}

impl Table {
    const MAX_PAGES: usize = 100;
    const MAX_ROWS: usize = Page::ROWS_PER_PAGE * Table::MAX_PAGES;

    fn new() -> Table{
        Table {
            num_rows: 0,
            pages: array![None; Table::MAX_PAGES],
        }
    }

    fn get_page(&mut self, page_num: usize) -> &mut Page {
        let mut page = &mut self.pages[page_num];
        if page.is_none() {
            *page = Some(Box::new(Page::create_leaf(None)));
        }
        self.pages[page_num].as_mut().unwrap()
    }

    fn get_cell(&mut self, row_num: usize) -> &mut Option<Cell> {
        assert!(row_num < Table::MAX_ROWS);

        let page_num = row_num / Page::ROWS_PER_PAGE;
        let row_num_in_page: usize = row_num % Page::ROWS_PER_PAGE;

        let page = self.get_page(page_num);

        match &mut page.node {
            Node::Internal(_) => unimplemented!(),
            Node::Leaf(leaf) => {
                &mut leaf.cells[row_num_in_page]
            }
        }
    }

    fn exec<W: Write>(&mut self, s: &Statement, output: &mut W) {
        match s {
            Statement::Insert(id, name, email) => {
                let mut row = Row {
                    id: *id,
                    username: [0u8; Row::USERNAME_SIZE],
                    email: [0u8; Row::EMAIL_SIZE],
                };

                let name = name.as_bytes();
                row.username[..name.len()].copy_from_slice(name);

                let email = email.as_bytes();
                row.email[..email.len()].copy_from_slice(email);

                let cell = self.get_cell(self.num_rows);
                *cell = Some(Cell {
                    key: *id,
                    row,
                });
                self.num_rows += 1;
            },
            Statement::Select => {
                writeln!(output, "{} rows:", self.num_rows).unwrap();
                for row_num in 0..self.num_rows {
                    let cell = self.get_cell(row_num).as_ref().unwrap();
                    writeln!(output, " {}", &cell.row).unwrap();
                }
            },
        }
    }
}

struct Repl<R: BufRead, W: Write> {
    table: Table,
    input: R,
    output: W,
}

impl<R: BufRead, W: Write> Repl<R,W> {
    fn new(input:R, output: W) -> Repl<R,W> {
        Repl {
            table: Table::new(),
            input,
            output,
        }
    }

    fn run(&mut self) -> i32 {

        loop
        {
            write!(&mut self.output, "db > ").unwrap();
            let mut line = String::new();
            match self.input.read_line(&mut line) {
                Ok(chars) => {
                    if chars == 0 {
                        return 0;
                    }

                    if let Some(c) = line.chars().next() {
                        if c == '.' {
                            let mut tokens = line.split_whitespace();
                            match tokens.next().unwrap() {
                                ".exit" => return 0,
                                _ => write!(&mut self.output, "Unknown command: {}", &line).unwrap(),
                            }
                        } else {
                            if let Some(stmt) = Statement::parse(&line) {
                                self.table.exec(&stmt, &mut self.output);
                            }
                        }
                    }
                },
                Err(e) => {
                    panic!("Failed to read from stdin: {}", e);
                },
                
            }
        }
    }
}

fn main() {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();

    let mut repl = Repl::new(stdin.lock(), stdout);
    process::exit(repl.run());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let input = b"select\n.exit\n";
        let mut output = Vec::new();
        let mut repl = Repl::new(&input[..], &mut output);
        assert_eq!(0, repl.run());
        assert_eq!("db > 0 rows:\ndb > ", String::from_utf8(output).unwrap());
    }

    #[test]
    fn echo() {
        let input = b"insert 11 john john@john.com\nselect\n.exit\n";
        let mut output = Vec::new();
        let mut repl = Repl::new(&input[..], &mut output);
        assert_eq!(0, repl.run());
        assert_eq!("db > db > 1 rows:\n [11 'john' 'john@john.com']\ndb > ", String::from_utf8(output).unwrap());
    }
}