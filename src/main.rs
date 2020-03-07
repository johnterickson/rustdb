#[macro_use]
extern crate array_macro;
#[macro_use]
extern crate static_assertions;

use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::Path;
use std::process;
use std::str::{from_utf8, FromStr};

type Id = u32;
type PageNumber = u32;
type CellNumber = u32;

const PAGE_SIZE: usize = 4096;

#[derive(Debug)]
enum TableError {
    Io(io::Error),
    IdAlreadyExists,
    TableFull,
    RootPageNotFound,
}

impl From<io::Error> for TableError {
    fn from(error: io::Error) -> Self {
        TableError::Io(error)
    }
}

type TableResult<T> = Result<T, TableError>;

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
            "[{:0>8} '{}' '{}']",
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
    children: Vec<InternalCell>,
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
            children: Vec::with_capacity(InternalNode::MAX_CELLS),
            right_child: None,
        }
    }

    fn find(&self, key: Id) -> (bool, CellNumber) {
        let cells = &self.children;
        let search_result = cells.binary_search_by(|probe| probe.key.cmp(&key));
        match search_result {
            Ok(found) => (true, found as CellNumber),
            Err(insert_here) => (false, insert_here as CellNumber),
        }
    }

    fn serialize<W: Write>(&self, mut writer: W) -> TableResult<()> {
        let mut child_count = [0u8; size_of::<u32>()];
        child_count[0..4].copy_from_slice(&(self.children.len() as u32).to_le_bytes());
        writer.write_all(&child_count)?;

        for child in self.children.iter() {
            child.serialize(&mut writer)?;
        }

        for _ in self.children.len()..InternalNode::MAX_CELLS {
            InternalCell::default().serialize(&mut writer)?
        }

        writer.write_all(&[0u8; InternalNode::WASTED_SPACE])?;
        Ok(())
    }

    fn deserialize<R: Read>(mut reader: R) -> TableResult<InternalNode> {
        let mut child_count = [0u8; size_of::<u32>()];
        reader.read_exact(&mut child_count[..])?;
        let child_count = u32::from_le_bytes(child_count);

        let mut node = InternalNode::create_empty();
        for i in 0..child_count {
            node.children.push(InternalCell::deserialize(&mut reader)?);
        }

        for _ in child_count as usize..InternalNode::MAX_CELLS {
            InternalCell::deserialize(&mut reader)?;
        }

        let mut padding = [0u8; InternalNode::WASTED_SPACE];
        reader.read_exact(&mut padding)?;

        Ok(node)
    }
}

struct LeafNode {
    cells: Vec<LeafCell>,
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
            cells: Vec::with_capacity(LeafNode::MAX_CELLS),
            next_leaf: 0,
        }
    }

    fn find(&self, key: Id) -> (bool, CellNumber) {
        let search_result = self.cells.binary_search_by(|probe| probe.key.cmp(&key));
        match search_result {
            Ok(found) => (true, found as CellNumber),
            Err(insert_here) => (false, insert_here as CellNumber),
        }
    }

    fn serialize<W: Write>(&self, mut writer: W) -> TableResult<()> {
        let mut cell_count = [0u8; size_of::<u32>()];
        cell_count[0..4].copy_from_slice(&(self.cells.len() as u32).to_le_bytes());
        writer.write_all(&cell_count)?;

        let mut next_leaf = [0u8; size_of::<u32>()];
        next_leaf[0..4].copy_from_slice(&self.next_leaf.to_le_bytes());
        writer.write_all(&next_leaf)?;

        for cell in &self.cells {
            cell.serialize(&mut writer)?;
        }

        for _ in self.cells.len()..LeafNode::MAX_CELLS {
            LeafCell::default().serialize(&mut writer)?
        }

        writer.write_all(&[0u8; LeafNode::WASTED_SPACE])?;
        Ok(())
    }

    fn deserialize<R: Read>(mut reader: R) -> TableResult<LeafNode> {
        let mut cell_count = [0u8; size_of::<u32>()];
        reader.read_exact(&mut cell_count[..])?;
        let cell_count = u32::from_le_bytes(cell_count);

        let mut next_leaf = [0u8; size_of::<u32>()];
        reader.read_exact(&mut next_leaf[..])?;
        let next_leaf = u32::from_le_bytes(next_leaf);

        let mut node = LeafNode::create_empty();
        for i in 0..cell_count {
            node.cells.push(LeafCell::deserialize(&mut reader)?);
        }
        for _ in cell_count as usize ..LeafNode::MAX_CELLS {
            LeafCell::deserialize(&mut reader)?;
        }
        node.next_leaf = next_leaf;

        let mut padding = [0u8; LeafNode::WASTED_SPACE];
        reader.read_exact(&mut padding)?;

        Ok(node)
    }
}

enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

impl Node {
    fn serialize<W: Write>(&self, writer: W) -> TableResult<()> {
        match self {
            Node::Internal(i) => i.serialize(writer),
            Node::Leaf(leaf) => leaf.serialize(writer),
        }
    }

    fn max_key(&self) -> Option<Id> {
        match self {
            Node::Internal(internal) => Some(
                internal.children.last().unwrap().key
            ),
            Node::Leaf(leaf) => {
                leaf.cells.last().map(|cell| cell.key)
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

    fn serialize<W: Write>(&self, mut writer: W) -> TableResult<()> {
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
        writer.write_all(&buf)?;
        self.node.serialize(&mut writer)
    }

    fn deserialize<R: Read>(mut reader: R) -> TableResult<Page> {
        let mut buf = [0u8; Page::HEADER_SIZE];
        reader.read_exact(&mut buf[..])?;
        let is_root = buf[1] == 1;
        let parent = if is_root {
            None
        } else {
            let mut parent = [0u8; size_of::<PageNumber>()];
            parent.copy_from_slice(&buf[2..=5]);
            Some(PageNumber::from_le_bytes(parent))
        };

        let node = match buf[0] {
            0 => Node::Internal(InternalNode::deserialize(reader)?),
            1 => Node::Leaf(LeafNode::deserialize(reader)?),
            node_type => panic!("unknown node type {}", node_type),
        };

        Ok(Page { node, parent })
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

    fn from_file(file: File) -> TableResult<Pager> {
        let file_length = file.metadata()?.len();
        assert!(file_length % PAGE_SIZE as u64 == 0);
        let page_count = file_length / PAGE_SIZE as u64;
        Ok(Pager {
            file,
            pages: array![None; Pager::MAX_PAGES as usize],
            file_length,
            page_count: page_count as PageNumber,
        })
    }

    fn open<P: AsRef<Path>>(path: P) -> TableResult<Pager> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Pager::from_file(file)
    }

    fn alloc(&mut self, p: Page) -> TableResult<(PageNumber, &mut Page)> {
        let page_num = self.page_count;
        if page_num == Pager::MAX_PAGES {
            return Err(TableError::TableFull);
        }

        let file_offset = (PAGE_SIZE as u64) * (page_num as u64);
        assert!(file_offset == self.file_length);

        let needed_file_length = file_offset + (PAGE_SIZE as u64);
        self.file.set_len(needed_file_length)?;
        self.file_length = needed_file_length;
        self.page_count += 1;
        assert!(self.pages[page_num as usize].is_none());
        let page = &mut self.pages[page_num as usize];
        *page = Some(Box::new(p));
        Ok((page_num, self.get_page(page_num)?.unwrap()))
    }

    fn get_page(&mut self, page_num: PageNumber) -> TableResult<Option<&mut Page>> {
        assert!(page_num < Pager::MAX_PAGES);

        let page = &mut self.pages[page_num as usize];

        if let Some(page) = page {
            Ok(Some(page))
        } else {
            // expand file as needed
            let file_offset = (PAGE_SIZE as u64) * (page_num as u64);
            let needed_file_length = file_offset + (PAGE_SIZE as u64);

            if needed_file_length > self.file_length {
                return Ok(None);
            }

            // read in an existing page
            self.file.seek(SeekFrom::Start(file_offset))?;
            let faulted_page = Page::deserialize(&mut self.file)?;

            *page = Some(Box::new(faulted_page));
            Ok(Some(page.as_mut().unwrap()))
        }
    }

    fn flush_all(&mut self) -> TableResult<()> {
        for page in self.pages.iter().enumerate() {
            if let (page_num, Some(page)) = page {
                let file_offset = (PAGE_SIZE as u64) * (page_num as u64);
                self.file.seek(SeekFrom::Start(file_offset))?;
                page.serialize(&mut self.file)?;
            }
        }

        self.file.flush()?;
        Ok(())
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        self.flush_all().unwrap();
    }
}

struct Cursor<'a> {
    table: &'a mut Table,
    page_index: PageNumber,
    cell_number: CellNumber,
    end_of_table: bool,
}

impl<'a> Cursor<'a> {
    fn start(table: &'a mut Table) -> TableResult<Cursor<'a>> {
        let (_, cursor) = Cursor::find(table, 0)?;
        Ok(cursor)
    }

    fn find_from_page(
        table: &'a mut Table,
        page_num: PageNumber,
        key: Id,
    ) -> TableResult<(bool, Cursor<'a>)> {
        match table.pager.get_page(page_num)?.unwrap().node {
            Node::Internal(ref internal) => {
                let (_, mut child_number) = internal.find(key);
                if child_number as usize == internal.children.len() {
                    child_number -= 1;
                }
                let child_cell = internal.children.get(child_number as usize).unwrap();
                let child_page_num = child_cell.child;
                Cursor::find_from_page(table, child_page_num, key)
            }
            Node::Leaf(ref leaf) => {
                let (found, cell_number) = leaf.find(key);
                let mut cursor = Cursor {
                    table,
                    page_index: page_num,
                    cell_number: cell_number,
                    end_of_table: false,
                };
                cursor.update_end_of_table()?;
                Ok((found, cursor))
            }
        }
    }

    /*
    Return the position of the given key.
    If the key is not present, return the position
    where it should be inserted
    */
    fn find(table: &'a mut Table, key: Id) -> TableResult<(bool, Cursor<'a>)> {
        Cursor::find_from_page(table, table.root_page, key)
    }

    fn page(&mut self) -> TableResult<&mut Page> {
        Ok(self.table.pager.get_page(self.page_index)?.unwrap())
    }

    fn cell(&mut self) -> TableResult<Option<&mut LeafCell>> {
        let cell_number = self.cell_number;
        Ok(match self.page()?.node {
            Node::Internal(_) => unimplemented!(),
            Node::Leaf(ref mut leaf) => leaf.cells.get_mut(cell_number as usize),
        })
    }

    fn update_end_of_table(&mut self) -> TableResult<()> {
        let cell_number = self.cell_number;
        self.end_of_table = match self.page()?.node {
            Node::Internal(_) => unreachable!(),
            Node::Leaf(ref leaf) => cell_number as usize >= leaf.cells.len(),
        };
        Ok(())
    }

    fn leaf_node(&mut self) -> TableResult<&LeafNode> {
        Ok(match self.page()?.node {
            Node::Internal(_) => unreachable!(),
            Node::Leaf(ref leaf) => leaf,
        })
    }

    fn internal_node_mut(&mut self) -> TableResult<&mut InternalNode> {
        Ok(match self.page()?.node {
            Node::Internal(ref mut i) => i,
            Node::Leaf(_) => unreachable!(),
        })
    }

    fn advance(&mut self) -> TableResult<()> {
        if self.end_of_table {
            return Ok(());
        }

        self.cell_number += 1;
        if (self.cell_number as usize) < self.leaf_node()?.cells.len() {
            return Ok(());
        }

        self.cell_number = 0;
        let next_leaf = self.leaf_node()?.next_leaf;
        self.end_of_table = next_leaf == 0;
        self.page_index = next_leaf;
        Ok(())
    }

    fn insert(&mut self, cell: LeafCell) -> TableResult<()> {
        let cell_number = self.cell_number as usize;

        // let's see if there's room in this node
        {
            match self.page()?.node {
                Node::Leaf(ref mut leaf) => {
                    if leaf.cells.len() < LeafNode::MAX_CELLS {
                        leaf.cells.insert(cell_number, cell);
                        self.table.row_count += 1;
                        self.table.validate()?;
                        return Ok(());
                    }
                }
                Node::Internal(_) => unimplemented!(),
            };
        }

        // we'll need to split the node
        // first, move the top half of cells to a new "right" node
        let mut right = LeafNode::create_empty();
        let orig_leaf_max = self.page()?.node.max_key().unwrap();
        {
            match self.page()?.node {
                Node::Leaf(ref mut leaf) => {
                    right.next_leaf = leaf.next_leaf;

                    right.cells = leaf.cells.split_off(LeafNode::LEFT_SPLIT_COUNT);
                    if cell_number < LeafNode::LEFT_SPLIT_COUNT {
                        right.cells.insert(0, leaf.cells.pop().unwrap());
                        leaf.cells.insert(cell_number, cell);
                    } else {
                        let cell_number = cell_number - LeafNode::LEFT_SPLIT_COUNT;
                        right.cells.insert(cell_number, cell);
                    }
                    assert_eq!(leaf.cells.len(), LeafNode::LEFT_SPLIT_COUNT);
                    assert_eq!(right.cells.len(), LeafNode::RIGHT_SPLIT_COUNT);
                }
                Node::Internal(_) => unimplemented!(),
            }
        }

        // next, we want to create a new parent that will point to this node and to new right node

        let original_parent = self.page()?.parent;
        let leaf_max = self.page()?.node.max_key().unwrap();
        let right = Node::Leaf(right);
        let right_max = right.max_key().unwrap();

        let left_page_num = self.page_index;
        let (right_page_num, _) = self.table.pager.alloc(Page {
            node: right,
            parent: None,
        })?;

        let parent_page_num = match original_parent {
            Some(parent) => {
                match self.table.pager.get_page(parent)?.as_mut().unwrap().node {
                    Node::Leaf(_) => unreachable!(),
                    Node::Internal(ref mut i) => {

                        if let Ok(left_child) = i.children.binary_search_by(|cell| cell.key.cmp(&orig_leaf_max)) {
                            i.children[left_child].key = leaf_max;
                        }

                        let search_result = i.children.binary_search_by(|probe| probe.key.cmp(&right_max));
                        let (_found, child_num) = match search_result {
                            Ok(i) => (true, i),
                            Err(i) => (false, i),
                        };

                        i.children.insert(child_num, InternalCell { key: right_max, child: right_page_num});
                    }
                }

                parent
            },
            None => {
                let mut new_internal_node = InternalNode::create_empty();
                new_internal_node.children.push(InternalCell {
                    key: leaf_max,
                    child: self.page_index,
                });
                new_internal_node.children.push(InternalCell {
                    key: right_max,
                    child: right_page_num,
                });
                new_internal_node.right_child = Some(right_page_num);
                let new_page = Page {
                    node: Node::Internal(new_internal_node),
                    parent: original_parent,
                };
                let (parent_page_num, _) = self.table.pager.alloc(new_page)?;
                let left = self.table.pager.get_page(left_page_num)?.unwrap();
                left.parent = Some(parent_page_num);

                parent_page_num
            } 
        };

        // update pointer
        let left = self.table.pager.get_page(left_page_num)?.unwrap();
        if let Node::Leaf(ref mut left) = left.node {
            left.next_leaf = right_page_num;
        } else {
            assert!(false);
        }

        self.table.pager.get_page(right_page_num)?.unwrap().parent = Some(parent_page_num);

        // update next leaf pointer

        if self.table.root_page == left_page_num {
            self.table.root_page = parent_page_num;
        }

        self.table.row_count += 1;
        self.table.validate()?;
        Ok(())
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
    row_count: Id,
}

impl Table {
    fn new(mut pager: Pager) -> TableResult<Table> {
        let mut root_page = None;

        let mut page_num = 0;
        while let Some(page) = pager.get_page(page_num)? {
            if page.parent.is_none() {
                root_page = Some(page_num);
                break;
            }
            page_num += 1;
        }

        let root_page = match root_page {
            Some(p) => p,
            None => {
                if page_num > 0 {
                    return Err(TableError::RootPageNotFound);
                } else {
                    let (new_root, _) = pager.alloc(Page::create_leaf(None))?;
                    new_root
                }
            }
        };

        let mut table = Table { root_page, pager, row_count: 0 };
        let mut row_count = 0;
        let mut cursor = Cursor::start(&mut table)?;
        while !cursor.end_of_table {
            cursor.advance()?;
            row_count += 1;
        }

        table.row_count = row_count;

        Ok(table)
    }

    fn exec<W: Write>(&mut self, s: &Statement, output: &mut W) -> TableResult<()> {
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

                let (found, mut cursor) = Cursor::find(self, *id)?;
                if found {
                    Err(TableError::IdAlreadyExists)
                } else {
                    cursor.insert(LeafCell { key: *id, row })
                }
            }
            Statement::Select => {
                let mut row_count = 0;
                let mut cursor = Cursor::start(self)?;
                while !cursor.end_of_table {
                    writeln!(output, " {}", cursor.cell()?.as_ref().unwrap().row)?;
                    cursor.advance()?;
                    row_count += 1;
                }
                writeln!(output, "{} rows.", row_count)?;
                Ok(())
            }
        }
    }

    fn validate(&mut self) -> TableResult<()> {
        let mut cursor = Cursor::start(self)?;
        let mut rows_found = 0;
        let mut last_key = None;
        while !cursor.end_of_table {
            rows_found += 1;

            let this_key = cursor.cell()?.as_ref().unwrap().key;
            assert!(last_key.is_none() || last_key.unwrap() < this_key);
            last_key = Some(this_key);

            match cursor.page()?.node {
                Node::Leaf(ref lnode) => {
                    let mut last_key = None;
                    for cell in &lnode.cells {
                        assert!(last_key.is_none() || last_key.unwrap() < cell.key);
                        last_key = Some(cell.key);
                    }
                },
                Node::Internal(ref inode) => {
                    let mut last_key = None;
                    for child in &inode.children {
                        assert!(last_key.is_none() || last_key.unwrap() < child.key);
                        last_key = Some(child.key);
                    }
                    assert_eq!(inode.right_child.unwrap(), inode.children.last().unwrap().child);
                }
            }

            cursor.advance()?;
        }

        assert_eq!(self.row_count, rows_found);
        Ok(())
    }
}

struct Repl<R: BufRead, W: Write> {
    table: Table,
    input: R,
    output: W,
}

impl<R: BufRead, W: Write> Repl<R, W> {
    fn new(input: R, output: W, pager: Pager) -> TableResult<Repl<R, W>> {
        Ok(Repl {
            table: Table::new(pager)?,
            input,
            output,
        })
    }

    fn run(&mut self) -> TableResult<i32> {
        loop {
            write!(&mut self.output, "db > ")?;
            self.output.flush()?;
            let mut line = String::new();
            match self.input.read_line(&mut line) {
                Ok(_) => {
                    writeln!(&mut self.output)?;

                    if let Some(c) = line.chars().next() {
                        if c == '.' {
                            let mut tokens = line.split_whitespace();
                            match tokens.next().unwrap() {
                                ".constants" => {
                                    writeln!(
                                        &mut self.output,
                                        "LeafNode::LEFT_SPLIT_COUNT: {}",
                                        &LeafNode::LEFT_SPLIT_COUNT
                                    )?;
                                    writeln!(
                                        &mut self.output,
                                        "LeafNode::RIGHT_SPLIT_COUNT: {}",
                                        &LeafNode::RIGHT_SPLIT_COUNT
                                    )?;
                                    writeln!(
                                        &mut self.output,
                                        "LeafNode::MAX_CELLS: {}",
                                        &LeafNode::MAX_CELLS
                                    )?;
                                }
                                ".exit" => return Ok(0),
                                _ => {
                                    write!(&mut self.output, "Unknown command: {}", &line)?;
                                }
                            }
                        } else {
                            if let Some(stmt) = Statement::parse(&line) {
                                if let Err(e) = self.table.exec(&stmt, &mut self.output) {
                                    writeln!(&mut self.output, "Error: {:?}", &e)?;
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

fn real_main() -> TableResult<i32> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();

    let pager = Pager::open("db.bin")?;
    let mut repl = Repl::new(stdin.lock(), stdout, pager)?;
    repl.run()
}

fn main() {
    process::exit(match real_main() {
        Ok(exit_code) => exit_code,
        Err(e) => {
            println!("Error: {:?}", &e);
            1
        }
    });
}

#[cfg(test)]
mod tests {
use super::*;
    use tempfile::tempfile;

    fn run_from_file<S: AsRef<str>>(file: File, input: &[S]) -> Vec<String> {
        let pager = Pager::from_file(file).unwrap();
        let mut input_bytes = Vec::new();
        for line in input {
            input_bytes.extend_from_slice(line.as_ref().as_bytes());
            input_bytes.push('\n' as u8);
        }
        let mut output = Vec::new();
        let mut repl = Repl::new(&input_bytes[..], &mut output, pager).unwrap();
        repl.run().unwrap();
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
            [" [00000011 'john' 'john@john.com']", "1 rows.",]
        );
    }

    fn persist_helper(keys: Vec<usize>) {
        let tempdb = tempfile::NamedTempFile::new().unwrap();
        let inserts: Vec<_> = keys.iter()
            .map(|i| format!("insert {} john{} john{}@john.com", i, i, i))
            .collect();
        let expected = {
            let mut expected: Vec<_> = keys.iter()
                .map(|i| format!(" [{:0>8} 'john{}' 'john{}@john.com']", i, i, i))
                .collect();
            expected.sort();
            expected.push(format!("{} rows.", keys.len()));
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
    fn full_leaf() {
        persist_helper((0..LeafNode::MAX_CELLS).collect());
        persist_helper((0..LeafNode::MAX_CELLS).rev().collect());
    }

    #[test]
    fn split_leaf() {
        persist_helper((0..=LeafNode::MAX_CELLS).collect());
        persist_helper((0..=LeafNode::MAX_CELLS).rev().collect());
    }

    #[test]
    fn split_leaf_twice() {
        persist_helper((0..(2*LeafNode::MAX_CELLS)).collect());
    }

    #[test]
    fn split_leaf_twice_backwards() {
        persist_helper((0..(2*LeafNode::MAX_CELLS)).rev().collect());
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
                " [00000011 'john11' 'john11@john.com']",
                "1 rows.",
            ]
        );
    }

    #[test]
    fn persist() {
        persist_helper( (11..=11).collect());
    }
}
