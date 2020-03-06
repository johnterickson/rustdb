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
struct LeafCell {
    key: Id,
    row: Row,
}

const_assert!(size_of::<LeafCell>() == size_of::<Id>() + size_of::<Row>());

impl LeafCell {
    const KEY_SIZE: usize = size_of::<Id>();
    const KEY_OFFSET: usize = 0;
    const VALUE_SIZE: usize = Row::SIZE;
    const VALUE_OFFSET: usize = LeafCell::KEY_OFFSET + LeafCell::KEY_SIZE;
    const SIZE: usize = LeafCell::VALUE_OFFSET + LeafCell::VALUE_SIZE;

    fn deserialize<R: Read>(r: &mut R) -> std::io::Result<LeafCell> {
        let mut id = [0u8; size_of::<Id>()];
        r.read_exact(&mut id)?;
        let row = Row::deserialize(r)?;
        Ok(LeafCell {
            key: Id::from_le_bytes(id),
            row,
        })
    }

    fn serialize<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        w.write_all(&self.key.to_le_bytes())?;
        self.row.serialize(w)
    }
}

#[derive(Debug, Default)]
struct InternalCell {
    key: Id,
    child: PageNumber,
}

impl InternalCell {
    const KEY_SIZE: usize = size_of::<Id>();
    const KEY_OFFSET: usize = 0;
    const CHILD_SIZE: usize = size_of::<PageNumber>();
    const CHILD_OFFSET: usize = InternalCell::KEY_OFFSET + InternalCell::KEY_SIZE;
    const SIZE: usize = InternalCell::CHILD_OFFSET + InternalCell::CHILD_SIZE;

    fn deserialize<R: Read>(r: &mut R) -> std::io::Result<InternalCell> {
        let mut id = [0u8; size_of::<Id>()];
        r.read_exact(&mut id)?;
        let mut page_num = [0u8; size_of::<PageNumber>()];
        r.read_exact(&mut page_num)?;
        Ok(InternalCell {
            key: Id::from_le_bytes(id),
            child: PageNumber::from_le_bytes(page_num),
        })
    }

    fn serialize<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        w.write_all(&self.key.to_le_bytes())?;
        w.write_all(&self.child.to_le_bytes())
    }
}

const_assert!(size_of::<InternalCell>() == size_of::<Id>() + size_of::<PageNumber>());

struct InternalNode {
    children: [Option<InternalCell>; InternalNode::MAX_CELLS],
    child_count: u32,
    right_child: Option<PageNumber>,
}

impl InternalNode {
    const HEADER_SIZE: usize = size_of::<u32>() + size_of::<PageNumber>();
    const SPACE_FOR_CELLS: usize = PAGE_SIZE - Page::HEADER_SIZE - InternalNode::HEADER_SIZE;
    const MAX_CELLS: usize = InternalNode::SPACE_FOR_CELLS / InternalCell::SIZE;
    const WASTED_SPACE: usize =
        InternalNode::SPACE_FOR_CELLS - (InternalNode::MAX_CELLS * InternalCell::SIZE);

    fn create_empty() -> InternalNode {
        InternalNode {
            children: array![None; InternalNode::MAX_CELLS],
            child_count: 0,
            right_child: None,
        }
    }

    fn find(&self, key: Id) -> (bool, CellNumber) {
        let cells = &self.children[0..self.child_count as usize];
        let search_result = cells.binary_search_by(|probe| probe.as_ref().unwrap().key.cmp(&key));
        match search_result {
            Ok(found) => (true, found as CellNumber),
            Err(insert_here) => (false, insert_here as CellNumber),
        }
    }

    fn serialize<W: Write>(&self, mut writer: W) {
        let mut child_count = [0u8; size_of::<u32>()];
        child_count[0..4].copy_from_slice(&self.child_count.to_le_bytes());
        writer.write_all(&child_count).unwrap();

        for child in self.children.iter() {
            child
                .as_ref()
                .unwrap_or(&InternalCell::default())
                .serialize(&mut writer)
                .unwrap();
        }

        writer
            .write_all(&[0u8; InternalNode::WASTED_SPACE])
            .unwrap();
    }

    fn deserialize<R: Read>(mut reader: R) -> InternalNode {
        let mut child_count = [0u8; size_of::<u32>()];
        reader.read_exact(&mut child_count[..]).unwrap();
        let child_count = u32::from_le_bytes(child_count);

        let mut node = InternalNode::create_empty();
        for i in 0..child_count {
            node.children[i as usize] = Some(InternalCell::deserialize(&mut reader).unwrap());
        }
        node.child_count = child_count;

        let mut padding = [0u8; InternalNode::WASTED_SPACE];
        reader.read_exact(&mut padding).unwrap();

        node
    }
}

struct LeafNode {
    cells: [Option<LeafCell>; LeafNode::MAX_CELLS],
    cell_count: u32,
    next_leaf: PageNumber,
}

impl LeafNode {
    const HEADER_SIZE: usize = size_of::<u32>() + size_of::<PageNumber>();
    const SPACE_FOR_CELLS: usize = PAGE_SIZE - Page::HEADER_SIZE - LeafNode::HEADER_SIZE;
    const MAX_CELLS: usize = LeafNode::SPACE_FOR_CELLS / LeafCell::SIZE;
    const WASTED_SPACE: usize = LeafNode::SPACE_FOR_CELLS - (LeafNode::MAX_CELLS * LeafCell::SIZE);
    const RIGHT_SPLIT_COUNT: usize = (LeafNode::MAX_CELLS + 1) / 2;
    const LEFT_SPLIT_COUNT: usize = LeafNode::MAX_CELLS + 1 - LeafNode::RIGHT_SPLIT_COUNT;

    fn create_empty() -> LeafNode {
        LeafNode {
            cells: array![None; LeafNode::MAX_CELLS],
            cell_count: 0,
            next_leaf: 0,
        }
    }

    fn find(&self, key: Id) -> (bool, CellNumber) {
        let cells = &self.cells[0..self.cell_count as usize];
        let search_result = cells.binary_search_by(|probe| probe.as_ref().unwrap().key.cmp(&key));
        match search_result {
            Ok(found) => (true, found as CellNumber),
            Err(insert_here) => (false, insert_here as CellNumber),
        }
    }

    fn serialize<W: Write>(&self, mut writer: W) {
        let mut cell_count = [0u8; size_of::<u32>()];
        cell_count[0..4].copy_from_slice(&self.cell_count.to_le_bytes());
        writer.write_all(&cell_count).unwrap();

        let mut next_leaf = [0u8; size_of::<u32>()];
        next_leaf[0..4].copy_from_slice(&self.next_leaf.to_le_bytes());
        writer.write_all(&next_leaf).unwrap();

        for cell in &self.cells {
            cell.as_ref()
                .unwrap_or(&LeafCell::default())
                .serialize(&mut writer)
                .unwrap();
        }

        writer.write_all(&[0u8; LeafNode::WASTED_SPACE]).unwrap();
    }

    fn deserialize<R: Read>(mut reader: R) -> LeafNode {
        let mut cell_count = [0u8; size_of::<u32>()];
        reader.read_exact(&mut cell_count[..]).unwrap();
        let cell_count = u32::from_le_bytes(cell_count);

        let mut next_leaf = [0u8; size_of::<u32>()];
        reader.read_exact(&mut next_leaf[..]).unwrap();
        let next_leaf = u32::from_le_bytes(next_leaf);

        let mut node = LeafNode::create_empty();
        for i in 0..cell_count {
            node.cells[i as usize] = Some(LeafCell::deserialize(&mut reader).unwrap());
        }
        node.cell_count = cell_count;
        node.next_leaf = next_leaf;

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
            Node::Internal(i) => i.serialize(writer),
            Node::Leaf(leaf) => leaf.serialize(writer),
        }
    }

    fn max_key(&self) -> Option<Id> {
        match self {
            Node::Internal(internal) => Some(
                internal.children[internal.child_count as usize - 1]
                    .as_ref()
                    .unwrap()
                    .key,
            ),
            Node::Leaf(leaf) => {
                if leaf.cell_count == 0 {
                    None
                } else {
                    Some(
                        leaf.cells[leaf.cell_count as usize - 1]
                            .as_ref()
                            .unwrap()
                            .key,
                    )
                }
            }
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
        let parent = if is_root {
            None
        } else {
            let mut parent = [0u8; size_of::<PageNumber>()];
            parent.copy_from_slice(&buf[2..=5]);
            Some(PageNumber::from_le_bytes(parent))
        };

        let node = match buf[0] {
            0 => Node::Internal(InternalNode::deserialize(reader)),
            1 => Node::Leaf(LeafNode::deserialize(reader)),
            node_type => panic!("unknown node type {}", node_type),
        };

        Page { node, parent }
    }
}

struct Pager {
    file: File,
    pages: [Option<Box<Page>>; Pager::MAX_PAGES as usize],
    file_length: u64,
    page_count: PageNumber,
}

impl Pager {
    const MAX_PAGES: PageNumber = 100;

    fn from_file(file: File) -> Pager {
        let file_length = file.metadata().unwrap().len();
        assert!(file_length % PAGE_SIZE as u64 == 0);
        let page_count = file_length / PAGE_SIZE as u64;
        Pager {
            file,
            pages: array![None; Pager::MAX_PAGES as usize],
            file_length,
            page_count: page_count as PageNumber,
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

    fn alloc(&mut self, p: Page) -> (PageNumber, &mut Page) {
        let page_num = self.page_count;

        let file_offset = (PAGE_SIZE as u64) * (page_num as u64);
        assert!(file_offset == self.file_length);

        let needed_file_length = file_offset + (PAGE_SIZE as u64);
        self.file.set_len(needed_file_length).unwrap();
        self.file_length = needed_file_length;
        self.page_count += 1;
        assert!(self.pages[page_num as usize].is_none());
        let page = &mut self.pages[page_num as usize];
        *page = Some(Box::new(p));
        (page_num, self.get_page(page_num).unwrap())
    }

    fn get_page(&mut self, page_num: PageNumber) -> Option<&mut Page> {
        assert!(page_num < Pager::MAX_PAGES);

        let page = &mut self.pages[page_num as usize];

        if let Some(page) = page {
            Some(page)
        } else {
            // expand file as needed
            let file_offset = (PAGE_SIZE as u64) * (page_num as u64);
            let needed_file_length = file_offset + (PAGE_SIZE as u64);

            if needed_file_length > self.file_length {
                return None;
            }

            // read in an existing page
            self.file.seek(SeekFrom::Start(file_offset)).unwrap();
            let faulted_page = Page::deserialize(&mut self.file);

            *page = Some(Box::new(faulted_page));
            Some(page.as_mut().unwrap())
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
    fn start(table: &'a mut Table) -> Cursor<'a> {
        let (_, cursor) = Cursor::find(table, 0);
        cursor
    }

    fn find_from_page(table: &'a mut Table, page_num: PageNumber, key: Id) -> (bool, Cursor<'a>) {
        match table.pager.get_page(page_num).unwrap().node {
            Node::Internal(ref internal) => {
                let (_, child_number) = internal.find(key);
                let child_cell = internal.children[child_number as usize].as_ref().unwrap();
                let child_page_num = child_cell.child;
                return Cursor::find_from_page(table, child_page_num, key);
            }
            Node::Leaf(ref leaf) => {
                let (found, cell_number) = leaf.find(key);
                let mut cursor = Cursor {
                    table,
                    page_index: page_num,
                    cell_number: cell_number,
                    end_of_table: false,
                };
                cursor.update_end_of_table();
                return (found, cursor);
            }
        };
    }

    /*
    Return the position of the given key.
    If the key is not present, return the position
    where it should be inserted
    */
    fn find(table: &'a mut Table, key: Id) -> (bool, Cursor<'a>) {
        Cursor::find_from_page(table, table.root_page, key)
    }

    fn page(&mut self) -> &mut Page {
        self.table.pager.get_page(self.page_index).unwrap()
    }

    fn cell(&mut self) -> &mut Option<LeafCell> {
        let cell_number = self.cell_number;
        match self.page().node {
            Node::Internal(_) => unimplemented!(),
            Node::Leaf(ref mut leaf) => &mut leaf.cells[cell_number as usize],
        }
    }

    fn update_end_of_table(&mut self) {
        let cell_number = self.cell_number;
        self.end_of_table = match self.page().node {
            Node::Internal(_) => unreachable!(),
            Node::Leaf(ref leaf) => cell_number >= leaf.cell_count,
        };
    }

    fn leaf_node(&mut self) -> &LeafNode {
        match self.page().node {
            Node::Internal(_) => unreachable!(),
            Node::Leaf(ref leaf) => leaf,
        }
    }

    fn advance(&mut self) {
        if self.end_of_table {
            return;
        }

        self.cell_number += 1;
        if self.cell_number < self.leaf_node().cell_count {
            return;
        }

        self.cell_number = 0;
        let next_leaf = self.leaf_node().next_leaf;
        self.end_of_table = next_leaf == 0;
        self.page_index = next_leaf;
    }

    fn insert(&mut self, cell: LeafCell) -> Result<(), TableError> {
        let cell_number = self.cell_number as usize;

        // let's see if there's room in this node
        {
            match self.page().node {
                Node::Leaf(ref mut leaf) => {
                    if leaf.cell_count < LeafNode::MAX_CELLS as u32 {
                        leaf.cells[cell_number..=leaf.cell_count as usize].rotate_right(1);
                        assert!(leaf.cells[cell_number].is_none());
                        leaf.cells[cell_number] = Some(cell);
                        leaf.cell_count += 1;
                        return Ok(());
                    }
                }
                Node::Internal(_) => unimplemented!(),
            };
        }

        // we'll need to split the node
        // first, move the top half of cells to a new "right" node
        let mut right = LeafNode::create_empty();
        let mut cell = Some(cell);
        {
            match self.page().node {
                Node::Leaf(ref mut leaf) => {
                    right.next_leaf = leaf.next_leaf;

                    for i in (0..=LeafNode::MAX_CELLS).rev() {
                        let dst_index = i % LeafNode::LEFT_SPLIT_COUNT;
                        if i >= LeafNode::LEFT_SPLIT_COUNT {
                            // goes in the right side
                            if i == cell_number as usize {
                                right.cells[dst_index] = Some(cell.take().unwrap());
                            } else if i > cell_number as usize {
                                right.cells[dst_index] =
                                    std::mem::replace(&mut leaf.cells[i - 1], None);
                            } else {
                                right.cells[dst_index] =
                                    std::mem::replace(&mut leaf.cells[i], None);
                            }
                        } else {
                            // goes stays in the left side
                            if i == cell_number as usize {
                                leaf.cells[dst_index] = Some(cell.take().unwrap());
                            } else if i > cell_number as usize {
                                leaf.cells[dst_index] =
                                    std::mem::replace(&mut leaf.cells[i - 1], None);
                            } else {
                                leaf.cells[dst_index] = std::mem::replace(&mut leaf.cells[i], None);
                            }
                        }
                    }
                    leaf.cell_count = LeafNode::LEFT_SPLIT_COUNT as u32;
                    right.cell_count = LeafNode::RIGHT_SPLIT_COUNT as u32;
                }
                Node::Internal(_) => unimplemented!(),
            }
        }

        // next, we want to create a new parent that will point to this node and to new right node

        let original_parent = self.page().parent;
        let left_max = self.page().node.max_key().unwrap();
        let right = Node::Leaf(right);
        let right_max = right.max_key().unwrap();

        let left_page_num = self.page_index;
        let (right_page_num, _) = self.table.pager.alloc(Page {
            node: right,
            parent: None,
        });

        let mut new_internal_node = InternalNode::create_empty();
        new_internal_node.child_count = 2;
        new_internal_node.children[0] = Some(InternalCell {
            key: left_max,
            child: self.page_index,
        });
        new_internal_node.children[1] = Some(InternalCell {
            key: right_max,
            child: right_page_num,
        });
        new_internal_node.right_child = Some(right_page_num);
        let new_page = Page {
            node: Node::Internal(new_internal_node),
            parent: original_parent,
        };

        // update parent and pointers
        let (new_page_num, _) = self.table.pager.alloc(new_page);
        {
            let left = self.table.pager.get_page(left_page_num).unwrap();
            left.parent = Some(new_page_num);
            if let Node::Leaf(ref mut left) = left.node {
                left.next_leaf = right_page_num;
            } else {
                assert!(false);
            }
        }
        self.table.pager.get_page(right_page_num).unwrap().parent = Some(new_page_num);

        // update next leaf pointer

        if self.table.root_page == left_page_num {
            self.table.root_page = new_page_num;
        }

        Ok(())
    }
}

// struct TableIterator<'a> {
//     table: &'a mut Table,
//     page_index: PageNumber,
//     cell_number: CellNumber,
//     first: bool,
// }

// impl<'a> Iterator for TableIterator<'a> {
//     type Item = &'a mut Option<Cell>;
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.first {
//             self.first = false;
//         } else {
//             self.cell_number += 1;
//         }

//         let page = &mut self.table.pager.get_page(self.page_index);
//         match page.node {
//             Node::Leaf(ref mut leaf) => {
//                 if self.cell_number < leaf.cell_count {
//                     Some(&mut leaf.cells[self.cell_number as usize])
//                 } else {
//                     None
//                 }
//             },
//             Node::Internal(_) => unimplemented!(),
//         }
//     }
// }

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

#[derive(Debug)]
enum TableError {
    IdAlreadyExists,
    TableFull,
}

struct Table {
    root_page: PageNumber,
    pager: Pager,
}

impl Table {
    fn new(mut pager: Pager) -> Table {
        // todo: keeping root at page_num 0 would be good

        let mut root_page = None;

        let mut page_num = 0;
        while let Some(page) = pager.get_page(page_num) {
            if page.parent.is_none() {
                root_page = Some(page_num);
                break;
            }
            page_num += 1;
        }

        let root_page = root_page.unwrap_or_else(|| {
            let (root_page, _) = pager.alloc(Page::create_leaf(None));
            root_page
        });

        Table { root_page, pager }
    }

    fn exec<W: Write>(&mut self, s: &Statement, output: &mut W) -> Result<(), TableError> {
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

                let (found, mut cursor) = Cursor::find(self, *id);
                if found {
                    Err(TableError::IdAlreadyExists)
                } else {
                    cursor.insert(LeafCell { key: *id, row })
                }
            }
            Statement::Select => {
                let mut row_count = 0;
                let mut cursor = Cursor::start(self);
                while !cursor.end_of_table {
                    writeln!(output, " {}", cursor.cell().as_ref().unwrap().row).unwrap();
                    cursor.advance();
                    row_count += 1;
                }
                writeln!(output, "{} rows.", row_count).unwrap();
                Ok(())
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
                                ".constants" => {
                                    writeln!(
                                        &mut self.output,
                                        "LeafNode::LEFT_SPLIT_COUNT: {}",
                                        &LeafNode::LEFT_SPLIT_COUNT
                                    )
                                    .unwrap();
                                    writeln!(
                                        &mut self.output,
                                        "LeafNode::RIGHT_SPLIT_COUNT: {}",
                                        &LeafNode::RIGHT_SPLIT_COUNT
                                    )
                                    .unwrap();
                                    writeln!(
                                        &mut self.output,
                                        "LeafNode::MAX_CELLS: {}",
                                        &LeafNode::MAX_CELLS
                                    )
                                    .unwrap();
                                }
                                ".exit" => return 0,
                                _ => {
                                    write!(&mut self.output, "Unknown command: {}", &line).unwrap()
                                }
                            }
                        } else {
                            if let Some(stmt) = Statement::parse(&line) {
                                if let Err(e) = self.table.exec(&stmt, &mut self.output) {
                                    writeln!(&mut self.output, "Error: {:?}", &e).unwrap();
                                }
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

    fn run_from_file<S: AsRef<str>>(file: File, input: &[S]) -> Vec<String> {
        let pager = Pager::from_file(file);
        let mut input_bytes = Vec::new();
        for line in input {
            input_bytes.extend_from_slice(line.as_ref().as_bytes());
            input_bytes.push('\n' as u8);
        }
        let mut output = Vec::new();
        let mut repl = Repl::new(&input_bytes[..], &mut output, pager);
        repl.run();
        let output = String::from_utf8(output).unwrap();
        output
            .lines()
            .filter(|line| line != &"db > ")
            .map(|line| line.to_owned())
            .collect()
    }

    fn run<S: AsRef<str>>(input: &[S]) -> Vec<String> {
        run_from_file(tempfile().unwrap(), input)
    }

    #[test]
    fn empty() {
        assert_eq!(run(&["select", ".exit"]), ["0 rows."]);
    }

    #[test]
    fn echo() {
        assert_eq!(
            run(&["insert 11 john john@john.com", "select", ".exit"]),
            [" [11 'john' 'john@john.com']", "1 rows.",]
        );
    }

    #[test]
    fn order() {
        assert_eq!(
            run(&[
                "insert 11 john11 john11@john.com",
                "insert 10 john10 john10@john.com",
                "insert 12 john12 john12@john.com",
                "insert 9 john9 john9@john.com",
                "select",
                ".exit"
            ]),
            [
                " [9 'john9' 'john9@john.com']",
                " [10 'john10' 'john10@john.com']",
                " [11 'john11' 'john11@john.com']",
                " [12 'john12' 'john12@john.com']",
                "4 rows.",
            ]
        );
    }

    #[test]
    fn split_leaf() {
        let tempdb = tempfile::NamedTempFile::new().unwrap();
        let inserts: Vec<_> = (0..=LeafNode::MAX_CELLS)
            .map(|i| format!("insert {} john{} john{}@john.com", i, i, i))
            .collect();
        let expected = {
            let mut expected: Vec<_> = (0..=LeafNode::MAX_CELLS)
                .map(|i| format!(" [{} 'john{}' 'john{}@john.com']", i, i, i))
                .collect();
            expected.push(format!("{} rows.", LeafNode::MAX_CELLS + 1));
            expected
        };

        {
            let mut commands = inserts.clone();
            commands.push("select".to_owned());
            commands.push(".exit".to_owned());
            assert_eq!(run_from_file(tempdb.reopen().unwrap(), &commands), expected);
        }

        {
            let mut commands = Vec::new();
            commands.push("select".to_owned());
            commands.push(".exit".to_owned());
            assert_eq!(run_from_file(tempdb.reopen().unwrap(), &commands), expected);
        }
    }

    #[test]
    fn duplicate() {
        assert_eq!(
            run(&[
                "insert 11 john11 john11@john.com",
                "insert 11 john11 john11@john.com",
                "select",
                ".exit"
            ]),
            [
                "Error: IdAlreadyExists",
                " [11 'john11' 'john11@john.com']",
                "1 rows.",
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
            [" [11 'john' 'john@john.com']", "1 rows.",]
        );
        assert_eq!(
            run_from_file(tempdb.reopen().unwrap(), &["select", ".exit"]),
            [" [11 'john' 'john@john.com']", "1 rows."]
        );
    }
}
