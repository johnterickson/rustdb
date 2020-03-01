#[macro_use]
extern crate array_macro;
#[macro_use]
extern crate static_assertions;

use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::Path;
use std::process;
use std::str::{from_utf8, FromStr};

type Id = u32;
type PageNumber = u32;
type CellNumber = u32;

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
        f.write_fmt(format_args!(
            "[{} '{}' '{}']",
            self.id,
            null_term_str(&self.username),
            null_term_str(&self.email)
        ))
    }
}

impl Default for Row {
    fn default() -> Self {
        Row {
            id: 0,
            username: [0; Row::USERNAME_SIZE],
            email: [0; Row::EMAIL_SIZE],
        }
    }
}

impl Row {
    const USERNAME_SIZE: usize = 32;
    const EMAIL_SIZE: usize = 256;
    const SIZE: usize = size_of::<Row>();

    fn deserialize<R: Read>(r: &mut R) -> std::io::Result<Row> {
        let mut id = [0u8; size_of::<Id>()];
        r.read_exact(&mut id)?;
        let id = Id::from_le_bytes(id);
        let mut row = Row {
            id,
            username: [0u8; Row::USERNAME_SIZE],
            email: [0u8; Row::EMAIL_SIZE],
        };
        r.read_exact(&mut row.username)?;
        r.read_exact(&mut row.email)?;
        Ok(row)
    }

    fn serialize<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        w.write_all(&self.id.to_le_bytes())?;
        w.write_all(&self.username)?;
        w.write_all(&self.email)?;
        Ok(())
    }
}

#[repr(C)]
#[derive(Default)]
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

    fn deserialize<R: Read>(r: &mut R) -> std::io::Result<Cell> {
        let mut id = [0u8; size_of::<Id>()];
        r.read_exact(&mut id)?;
        let row = Row::deserialize(r)?;
        Ok(Cell {
            key: Id::from_le_bytes(id),
            row,
        })
    }

    fn serialize<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        w.write_all(&self.key.to_le_bytes())?;
        self.row.serialize(w)
    }
}

struct InternalNode {}

struct LeafNode {
    cells: [Option<Cell>; LeafNode::MAX_CELLS],
    cell_count: u32,
}

impl LeafNode {
    const HEADER_SIZE: usize = size_of::<u32>();
    const SPACE_FOR_CELLS: usize = PAGE_SIZE - Page::HEADER_SIZE - LeafNode::HEADER_SIZE;
    const MAX_CELLS: usize = LeafNode::SPACE_FOR_CELLS / Cell::SIZE;
    const WASTED_SPACE: usize = LeafNode::SPACE_FOR_CELLS - (LeafNode::MAX_CELLS * Cell::SIZE);

    fn create_empty() -> LeafNode {
        LeafNode {
            cells: array![None; LeafNode::MAX_CELLS],
            cell_count: 0,
        }
    }

    fn serialize<W: Write>(&self, mut writer: W) {
        let mut cell_count = [0u8; size_of::<u32>()];
        cell_count[0..4].copy_from_slice(&self.cell_count.to_le_bytes());
        writer.write_all(&cell_count).unwrap();

        for cell in &self.cells {
            cell.as_ref()
                .unwrap_or(&Cell::default())
                .serialize(&mut writer)
                .unwrap();
        }

        writer.write_all(&[0u8; LeafNode::WASTED_SPACE]).unwrap();
    }

    fn deserialize<R: Read>(mut reader: R) -> LeafNode {
        let mut cell_count = [0u8; size_of::<u32>()];
        reader.read_exact(&mut cell_count[..]).unwrap();
        let cell_count = u32::from_le_bytes(cell_count);

        let mut node = LeafNode::create_empty();
        for i in 0..cell_count {
            node.cells[i as usize] = Some(Cell::deserialize(&mut reader).unwrap());
        }
        node.cell_count = cell_count;

        let mut padding = [0u8; LeafNode::WASTED_SPACE];
        reader.read_exact(&mut padding).unwrap();

        node
    }
}

enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

impl Node {
    fn serialize<W: Write>(&self, writer: W) {
        match self {
            Node::Internal(_) => unimplemented!(),
            Node::Leaf(leaf) => leaf.serialize(writer),
        }
    }
}

struct Page {
    node: Node,
    parent: Option<PageNumber>,
}

impl Page {
    const HEADER_SIZE: usize = 6;

    fn create_leaf(parent: Option<PageNumber>) -> Page {
        Page {
            node: Node::Leaf(LeafNode::create_empty()),
            parent,
        }
    }

    fn serialize<W: Write>(&self, mut writer: W) {
        let mut buf = [0u8; Page::HEADER_SIZE];
        buf[0] = match self.node {
            Node::Internal(_) => 0,
            Node::Leaf(_) => 1,
        };
        buf[1] = match self.parent {
            Some(_) => 0,
            None => 1,
        };
        buf[2..].copy_from_slice(&self.parent.unwrap_or(0).to_le_bytes());
        writer.write_all(&buf).unwrap();
        self.node.serialize(&mut writer);
    }

    fn deserialize<R: Read>(mut reader: R) -> Page {
        let mut buf = [0u8; Page::HEADER_SIZE];
        reader.read_exact(&mut buf[..]).unwrap();
        let is_root = buf[1] == 1;
        assert!(is_root);
        let node = match buf[0] {
            0 => {
                todo!();
                //Node::Internal(InternalNode())
            }
            1 => Node::Leaf(LeafNode::deserialize(reader)),
            node_type => panic!("unknown node type {}", node_type),
        };

        Page { node, parent: None }
    }
}

struct Pager {
    file: File,
    pages: [Option<Box<Page>>; Pager::MAX_PAGES as usize],
    file_length: u64,
}

impl Pager {
    const MAX_PAGES: PageNumber = 100;

    fn from_file(file: File) -> Pager {
        let file_length = file.metadata().unwrap().len();
        Pager {
            file,
            pages: array![None; Pager::MAX_PAGES as usize],
            file_length,
        }
    }

    fn open<P: AsRef<Path>>(path: P) -> Pager {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        Pager::from_file(file)
    }

    fn get_page(&mut self, page_num: PageNumber) -> &mut Page {
        assert!(page_num < Pager::MAX_PAGES);

        let page = &mut self.pages[page_num as usize];

        if let Some(page) = page {
            page
        } else {
            // expand file as needed
            let file_offset = (PAGE_SIZE as u64) * (page_num as u64);
            let needed_file_length = file_offset + (PAGE_SIZE as u64);

            let faulted_page = if needed_file_length > self.file_length {
                // create a new page
                assert!(needed_file_length == self.file_length + (PAGE_SIZE as u64));
                self.file.set_len(needed_file_length).unwrap();
                self.file_length = needed_file_length;
                Page::create_leaf(None)
            } else {
                // read in an existing page
                self.file.seek(SeekFrom::Start(file_offset)).unwrap();
                Page::deserialize(&mut self.file)
            };

            *page = Some(Box::new(faulted_page));
            page.as_mut().unwrap()
        }
    }

    fn flush_all(&mut self) {
        for page in self.pages.iter().enumerate() {
            if let (page_num, Some(page)) = page {
                let file_offset = (PAGE_SIZE as u64) * (page_num as u64);
                self.file.seek(SeekFrom::Start(file_offset)).unwrap();
                page.serialize(&mut self.file);
            }
        }

        self.file.flush().unwrap();
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        self.flush_all();
    }
}

struct Cursor<'a> {
    table: &'a mut Table,
    page_index: PageNumber,
    cell_number: CellNumber,
    end_of_table: bool,
}

impl<'a> Cursor<'a> {
    fn table_start(table: &'a mut Table) -> Cursor<'a> {
        let root_page = table.root_page;
        let mut cursor = Cursor {
            table,
            page_index: root_page,
            cell_number: 0,
            end_of_table: false,
        };

        cursor.update_end_of_table();

        cursor
    }

    fn table_end(table: &'a mut Table) -> Cursor<'a> {
        let root_page = table.root_page;
        let mut cursor = Cursor {
            table,
            page_index: root_page,
            cell_number: 0,
            end_of_table: true,
        };

        cursor.cell_number = match cursor.page().node {
            Node::Internal(_) => unimplemented!(),
            Node::Leaf(ref leaf) => leaf.cell_count,
        };

        assert!(cursor.cell().is_none());

        cursor
    }

    fn page(&mut self) -> &mut Page {
        self.table.pager.get_page(self.page_index)
    }

    fn cell(&mut self) -> &mut Option<Cell> {
        let cell_number = self.cell_number;
        match self.page().node {
            Node::Internal(_) => unimplemented!(),
            Node::Leaf(ref mut leaf) => &mut leaf.cells[cell_number as usize],
        }
    }

    fn update_end_of_table(&mut self) {
        self.end_of_table = self.cell_number
            >= match self.page().node {
                Node::Internal(_) => unimplemented!(),
                Node::Leaf(ref leaf) => leaf.cell_count,
            };
    }

    fn advance(&mut self) {
        self.cell_number += 1;
        self.update_end_of_table();
    }

    fn insert(&mut self, cell: Cell) {
        assert!(self.cell().is_none());
        *self.cell() = Some(cell);
        match self.page().node {
            Node::Leaf(ref mut leaf) => leaf.cell_count += 1,
            Node::Internal(_) => unimplemented!(),
        };
    }
}

enum Statement {
    Insert(Id, String, String),
    Select,
}

impl Statement {
    fn parse(line: &str) -> Option<Statement> {
        let mut tokens = line.split_whitespace();
        match tokens.next().unwrap() {
            "insert" => {
                let id =
                    Id::from_str(tokens.next().expect("id not given")).expect("could not parse id");
                let username = tokens.next().expect("username not given").to_owned();
                let email = tokens.next().expect("email not given").to_owned();
                Some(Statement::Insert(id, username, email))
            }
            "select" => Some(Statement::Select),
            command => {
                eprintln!("Unknown command: {}", &command);
                None
            }
        }
    }
}

struct Table {
    root_page: PageNumber,
    pager: Pager,
}

impl Table {
    fn new(pager: Pager) -> Table {
        Table {
            root_page: 0,
            pager,
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

                let mut cursor = Cursor::table_end(self);
                cursor.insert(Cell { key: *id, row });
            }
            Statement::Select => {
                let mut row_count = 0;
                let mut cursor = Cursor::table_start(self);
                while !cursor.end_of_table {
                    writeln!(output, " {}", cursor.cell().as_ref().unwrap().row).unwrap();
                    cursor.advance();
                    row_count += 1;
                }
                writeln!(output, "{} rows.", row_count).unwrap();
            }
        }
    }
}

struct Repl<R: BufRead, W: Write> {
    table: Table,
    input: R,
    output: W,
}

impl<R: BufRead, W: Write> Repl<R, W> {
    fn new(input: R, output: W, pager: Pager) -> Repl<R, W> {
        Repl {
            table: Table::new(pager),
            input,
            output,
        }
    }

    fn run(&mut self) -> i32 {
        loop {
            write!(&mut self.output, "db > ").unwrap();
            self.output.flush().unwrap();
            let mut line = String::new();
            match self.input.read_line(&mut line) {
                Ok(_) => {
                    writeln!(&mut self.output).unwrap();

                    if let Some(c) = line.chars().next() {
                        if c == '.' {
                            let mut tokens = line.split_whitespace();
                            match tokens.next().unwrap() {
                                ".exit" => return 0,
                                _ => {
                                    write!(&mut self.output, "Unknown command: {}", &line).unwrap()
                                }
                            }
                        } else {
                            if let Some(stmt) = Statement::parse(&line) {
                                self.table.exec(&stmt, &mut self.output);
                            }
                        }
                    }
                }
                Err(e) => {
                    panic!("Failed to read from stdin: {}", e);
                }
            }
        }
    }
}

fn real_main() -> i32 {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();

    let pager = Pager::open("db.bin");
    let mut repl = Repl::new(stdin.lock(), stdout, pager);
    repl.run()
}

fn main() {
    process::exit(real_main());
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempfile;

    fn run_from_file(file: File, input: &[&str]) -> Vec<String> {
        let pager = Pager::from_file(file);
        let mut input_bytes = Vec::new();
        for line in input {
            input_bytes.extend_from_slice(line.as_bytes());
            input_bytes.push('\n' as u8);
        }
        let mut output = Vec::new();
        let mut repl = Repl::new(&input_bytes[..], &mut output, pager);
        repl.run();
        let output = String::from_utf8(output).unwrap();
        output.lines().map(|line| line.to_owned()).collect()
    }

    fn run(input: &[&str]) -> Vec<String> {
        run_from_file(tempfile().unwrap(), input)
    }

    #[test]
    fn empty() {
        assert_eq!(run(&["select", ".exit"]), ["db > ", "0 rows.", "db > "]);
    }

    #[test]
    fn echo() {
        assert_eq!(
            run(&["insert 11 john john@john.com", "select", ".exit"]),
            [
                "db > ",
                "db > ",
                " [11 'john' 'john@john.com']",
                "1 rows.",
                "db > "
            ]
        );
    }

    #[test]
    fn persist() {
        let tempdb = tempfile::NamedTempFile::new().unwrap();
        assert_eq!(
            run_from_file(
                tempdb.reopen().unwrap(),
                &["insert 11 john john@john.com", "select", ".exit"]
            ),
            [
                "db > ",
                "db > ",
                " [11 'john' 'john@john.com']",
                "1 rows.",
                "db > "
            ]
        );
        assert_eq!(
            run_from_file(tempdb.reopen().unwrap(), &["select", ".exit"]),
            ["db > ", " [11 'john' 'john@john.com']", "1 rows.", "db > "]
        );
    }
}
