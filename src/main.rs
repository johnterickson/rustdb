#[macro_use]
extern crate static_assertions;

mod locks;
use locks::*;

mod pager;
use pager::*;

mod serialize;
use serialize::*;

use owning_ref::{OwningRef, OwningRefMut};
use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::Path;
use std::process;
use std::{
    ops::{Deref, DerefMut},
    str::{from_utf8, FromStr},
};

type Id = u32;
type PageNumber = u32;
type CellNumber = u32;

const PAGE_SIZE: usize = 4096;

#[derive(Debug)]
pub enum TableError {
    Io(io::Error),
    IdAlreadyExists,
    CannotSplitInternalNodes,
    RootPageNotFound,
    PageNotFound,
    CellNotFound,
}

impl From<io::Error> for TableError {
    fn from(error: io::Error) -> Self {
        TableError::Io(error)
    }
}

pub type TableResult<T> = Result<T, TableError>;

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
}

#[repr(C)]
#[derive(Default)]
pub struct LeafCell {
    key: Id,
    row: Row,
}

impl Debug for LeafCell {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self))
    }
}

impl Display for LeafCell {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:0>8}:[{}]", self.key, self.row,))
    }
}

const_assert!(size_of::<LeafCell>() == size_of::<Id>() + size_of::<Row>());

impl LeafCell {
    const KEY_SIZE: usize = size_of::<Id>();
    const KEY_OFFSET: usize = 0;
    const VALUE_SIZE: usize = Row::SIZE;
    const VALUE_OFFSET: usize = LeafCell::KEY_OFFSET + LeafCell::KEY_SIZE;
    const SIZE: usize = LeafCell::VALUE_OFFSET + LeafCell::VALUE_SIZE;
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
}

const_assert!(size_of::<InternalCell>() == size_of::<Id>() + size_of::<PageNumber>());

pub struct InternalNode {
    children: Vec<InternalCell>,
    right_child: Option<PageNumber>,
}

impl InternalNode {
    pub const HEADER_SIZE: usize = size_of::<u32>() + size_of::<PageNumber>();
    pub const SPACE_FOR_CELLS: usize = PAGE_SIZE - Page::HEADER_SIZE - Self::HEADER_SIZE;
    pub const MAX_CELLS: usize = Self::SPACE_FOR_CELLS / InternalCell::SIZE;
    pub const WASTED_SPACE: usize = Self::SPACE_FOR_CELLS - (Self::MAX_CELLS * InternalCell::SIZE);
    pub const RIGHT_SPLIT_COUNT: usize = (Self::MAX_CELLS + 1) / 2;
    pub const LEFT_SPLIT_COUNT: usize = Self::MAX_CELLS + 1 - Self::RIGHT_SPLIT_COUNT;

    fn create_empty() -> Self {
        InternalNode {
            children: Vec::with_capacity(Self::MAX_CELLS),
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
}

pub struct LeafNode {
    cells: Vec<LeafCell>,
    next_leaf: PageNumber,
}

impl LeafNode {
    const HEADER_SIZE: usize = size_of::<u32>() + size_of::<PageNumber>();
    const SPACE_FOR_CELLS: usize = PAGE_SIZE - Page::HEADER_SIZE - Self::HEADER_SIZE;
    const MAX_CELLS: usize = Self::SPACE_FOR_CELLS / LeafCell::SIZE;
    const WASTED_SPACE: usize = Self::SPACE_FOR_CELLS - (Self::MAX_CELLS * LeafCell::SIZE);
    const RIGHT_SPLIT_COUNT: usize = (Self::MAX_CELLS + 1) / 2;
    const LEFT_SPLIT_COUNT: usize = Self::MAX_CELLS + 1 - Self::RIGHT_SPLIT_COUNT;

    fn create_empty() -> Self {
        LeafNode {
            cells: Vec::with_capacity(Self::MAX_CELLS),
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
}

pub enum Node {
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
            Node::Internal(internal) => Some(internal.children.last().unwrap().key),
            Node::Leaf(leaf) => leaf.cells.last().map(|cell| cell.key),
        }
    }

    fn as_internal(&self) -> &InternalNode {
        match self {
            Node::Internal(ref i) => i,
            _ => panic!("Not an internal node."),
        }
    }

    fn as_leaf_mut(&mut self) -> &mut LeafNode {
        match self {
            Node::Leaf(ref mut leaf) => leaf,
            _ => panic!("Not a leaf node."),
        }
    }
}

type TableReadUpgradeGuard<'a> = RwLockUpgradableReadGuard<'a, TableImpl>;

enum FindResult {
    Recurse(PageNumber),
    Done((bool, CellNumber)),
}

struct Cursor<T: Deref<Target = TableImpl>> {
    table: Option<T>,
    page_index: PageNumber,
    cell_number: CellNumber,
    end_of_table: bool,
}

impl<T> Cursor<T>
where
    T: Deref<Target = TableImpl>,
{
    fn start(table: T) -> TableResult<Cursor<T>> {
        let (_, cursor) = Cursor::<T>::find(table, 0)?;
        Ok(cursor)
    }

    fn find_from_page(table: T, page_num: PageNumber, key: Id) -> TableResult<(bool, Cursor<T>)> {
        let recurse = match table.pager.get_page(page_num)?.node {
            Node::Internal(ref internal) => {
                let (_, mut child_number) = internal.find(key);
                if child_number as usize == internal.children.len() {
                    child_number -= 1;
                }
                let child_cell = internal.children.get(child_number as usize).unwrap();
                let child_page_num = child_cell.child;
                FindResult::Recurse(child_page_num)
            }
            Node::Leaf(ref leaf) => FindResult::Done(leaf.find(key)),
        };

        Ok(match recurse {
            FindResult::Recurse(child_page_num) => {
                Cursor::find_from_page(table, child_page_num, key)?
            }
            FindResult::Done((found, cell_number)) => {
                let mut cursor = Cursor {
                    table: Some(table),
                    page_index: page_num,
                    cell_number,
                    end_of_table: false,
                };
                cursor.update_end_of_table()?;
                (found, cursor)
            }
        })
    }

    /*
    Return the position of the given key.
    If the key is not present, return the position
    where it should be inserted
    */
    fn find(table: T, key: Id) -> TableResult<(bool, Cursor<T>)> {
        let page_num = table.root_page;
        Cursor::<T>::find_from_page(table, page_num, key)
    }

    fn page(&self) -> TableResult<PageReadGuard> {
        self.table.as_ref().unwrap().pager.get_page(self.page_index)
    }

    fn page_mut(&self) -> TableResult<PageWriteGuard> {
        self.table
            .as_ref()
            .unwrap()
            .pager
            .get_page_mut(self.page_index)
    }

    fn cell(&self) -> TableResult<CellWriteGuard> {
        let cell_number = self.cell_number;
        let page = self.page_mut()?;
        page.try_map_mut(|page| match page.node {
            Node::Internal(_) => unimplemented!(),
            Node::Leaf(ref mut leaf) => match leaf.cells.get_mut(cell_number as usize) {
                None => Err(TableError::CellNotFound),
                Some(cell) => Ok(cell),
            },
        })
    }

    fn update_end_of_table(&mut self) -> TableResult<()> {
        let cell_number = self.cell_number;
        self.end_of_table = {
            let x = match self.page()?.node {
                Node::Internal(_) => unreachable!(),
                Node::Leaf(ref leaf) => cell_number as usize >= leaf.cells.len(),
            };
            x
        };

        Ok(())
    }

    fn leaf_node(&self) -> TableResult<LeafNodeReadGuard> {
        let page = self.page()?;
        Ok(page.map(|page| match page.node {
            Node::Internal(_) => unreachable!(),
            Node::Leaf(ref leaf) => leaf,
        }))
    }

    fn leaf_node_mut(&self) -> TableResult<LeafNodeWriteGuard> {
        let page = self.page_mut()?;
        Ok(page.map_mut(|page| match page.node {
            Node::Internal(_) => unreachable!(),
            Node::Leaf(ref mut leaf) => leaf,
        }))
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
}

impl<T> Cursor<T>
where
    T: ReadUpgradeGuard<TableImpl>,
{
    fn insert(mut self, cell: LeafCell) -> TableResult<()> {
        let cell_number = self.cell_number as usize;

        // let's see if there's room in this node
        {
            let mut leaf = self.leaf_node_mut()?;
            if leaf.cells.len() < LeafNode::MAX_CELLS {
                leaf.cells.insert(cell_number, cell);
                drop(leaf);
                Table::validate(self.table.unwrap())?;
                return Ok(());
            }
        }

        // we'll need to allocate some pages - let's see if we have room...
        let original_parent = self.page()?.parent;
        if let Some(original_parent) = original_parent {
            let table = self.table.as_ref().unwrap();
            let parent_page = table.pager.get_page_mut(original_parent)?;
            if parent_page.node.as_internal().children.len() == InternalNode::MAX_CELLS {
                return Err(TableError::CannotSplitInternalNodes);
            }
        }

        let mut cursor = Cursor {
            table: None,
            page_index: self.page_index,
            cell_number: self.cell_number,
            end_of_table: self.end_of_table,
        };

        let read_guard = self.table.take().unwrap();
        let write_guard = read_guard.upgrade_to_write();

        cursor.table = Some(write_guard);

        cursor.insert_write(cell)
    }
}

impl<T: DerefMut<Target = TableImpl>> Cursor<T> {
    fn insert_write(mut self, cell: LeafCell) -> TableResult<()> {
        let cell_number = self.cell_number as usize;

        // let's see if there's room in this node
        {
            let mut leaf = self.leaf_node_mut()?;
            if leaf.cells.len() < LeafNode::MAX_CELLS {
                leaf.cells.insert(cell_number, cell);
                drop(leaf);
                Table::validate(self.table.unwrap())?;
                return Ok(());
            }
        }

        // we'll need to allocate some pages - let's see if we have room...
        let original_parent = self.page()?.parent;
        if let Some(original_parent) = original_parent {
            let table = self.table.as_ref().unwrap();
            let parent_page = table.pager.get_page_mut(original_parent)?;
            if parent_page.node.as_internal().children.len() == InternalNode::MAX_CELLS {
                return Err(TableError::CannotSplitInternalNodes);
            }
        }

        // let mut table = self.table.as_ref().unwrap();

        // we'll need to split the node
        // first, move the top half of cells to a new "right" node
        let mut right = LeafNode::create_empty();
        let orig_leaf_max = self.page()?.node.max_key().unwrap();
        {
            let mut leaf = self.leaf_node_mut()?;
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

        // next, we want to create a new parent that will point to this node and to new right node

        let leaf_max = self.page()?.node.max_key().unwrap();
        let right = Node::Leaf(right);
        let right_max = right.max_key().unwrap();

        let left_page_num = self.page_index;
        let right_page_num = self.table.as_mut().unwrap().pager.alloc(Page {
            node: right,
            parent: None,
        })?;

        let parent_page_num = match original_parent {
            Some(parent) => {
                match self
                    .table
                    .as_ref()
                    .unwrap()
                    .pager
                    .get_page_mut(parent)?
                    .node
                {
                    Node::Leaf(_) => unreachable!(),
                    Node::Internal(ref mut i) => {
                        if let Ok(left_child) = i
                            .children
                            .binary_search_by(|cell| cell.key.cmp(&orig_leaf_max))
                        {
                            i.children[left_child].key = leaf_max;
                        }

                        let search_result = i
                            .children
                            .binary_search_by(|probe| probe.key.cmp(&right_max));
                        let (_found, child_num) = match search_result {
                            Ok(i) => (true, i),
                            Err(i) => (false, i),
                        };

                        i.children.insert(
                            child_num,
                            InternalCell {
                                key: right_max,
                                child: right_page_num,
                            },
                        );
                    }
                }

                parent
            }
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
                let table = self.table.as_mut().unwrap();
                let parent_page_num = table.pager.alloc(new_page)?;
                let mut left = table.pager.get_page_mut(left_page_num)?;
                left.parent = Some(parent_page_num);

                parent_page_num
            }
        };

        // update pointer
        {
            let mut left = self
                .table
                .as_ref()
                .unwrap()
                .pager
                .get_page_mut(left_page_num)?;
            left.node.as_leaf_mut().next_leaf = right_page_num;

            if let Node::Leaf(ref mut left) = left.node {
                left.next_leaf = right_page_num;
            } else {
                panic!("Expected");
            }
        }

        {
            let table = self.table.as_mut().unwrap();
            table.pager.get_page_mut(right_page_num)?.parent = Some(parent_page_num);

            // update next leaf pointer

            if table.root_page == left_page_num {
                table.root_page = parent_page_num;
            }
        }

        Table::validate(self.table.unwrap())?;
        Ok(())
    }
}

enum Statement {
    Insert(Id, String, String),
    Select,
    Print,
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
            "print" => Some(Statement::Print),
            command => {
                eprintln!("Unknown command: {}", &command);
                None
            }
        }
    }
}

struct TableImpl {
    root_page: PageNumber,
    pager: Pager,
}

struct Table(RwLock<TableImpl>);

impl Table {
    fn new(mut pager: Pager) -> TableResult<Table> {
        let page_count = pager.page_count();
        let root_page = if page_count == 0 {
            pager.alloc(Page::create_leaf(None))?
        } else {
            let mut root_page = None;
            for page_num in 0..page_count {
                let page = pager.get_page(page_num)?;
                if page.parent.is_none() {
                    root_page = Some(page_num);
                    break;
                }
            }
            if let Some(root_page) = root_page {
                root_page
            } else {
                return Err(TableError::RootPageNotFound);
            }
        };

        let table = TableImpl { root_page, pager };

        Ok(Table(RwLock::new(table)))
    }

    fn print_tree<W: Write, T: ReadGuard<TableImpl>>(
        table: &T,
        output: &mut W,
        page_num: PageNumber,
        indent: usize,
    ) -> TableResult<()> {
        for _ in 0..indent {
            write!(output, " ")?;
        }
        let page = table.pager.get_page(page_num)?;
        write!(output, "page: {} parent: {:?}", page_num, page.parent)?;
        match page.node {
            Node::Internal(ref internal) => {
                writeln!(output, " internal")?;
                for child in &internal.children {
                    Table::print_tree(table, output, child.child, indent + 1)?;
                }
            }
            Node::Leaf(ref leaf) => {
                writeln!(output, " leaf {:?}", leaf.cells)?;
            }
        }

        Ok(())
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

                let read_guard = self.0.upgradable_read();
                let (found, cursor) = Cursor::find(read_guard, *id)?;
                if found {
                    Err(TableError::IdAlreadyExists)
                } else {
                    cursor.insert(LeafCell { key: *id, row })
                }
            }
            Statement::Select => {
                let mut row_count = 0;
                let read_guard = self.0.upgradable_read();
                let mut cursor = Cursor::<TableReadUpgradeGuard<'_>>::start(read_guard)?;
                while !cursor.end_of_table {
                    writeln!(output, " {}", cursor.cell()?.row)?;
                    cursor.advance()?;
                    row_count += 1;
                }
                writeln!(output, "{} rows.", row_count)?;
                Ok(())
            }
            Statement::Print => {
                let read_guard = self.0.read();
                Table::print_tree(&read_guard, output, read_guard.root_page, 0)?;
                Ok(())
            }
        }
    }

    fn validate<T: Deref<Target = TableImpl>>(table: T) -> TableResult<()> {
        let mut cursor = Cursor::start(table)?;
        let mut last_key = None;
        while !cursor.end_of_table {
            let this_key = cursor.cell()?.key;
            assert!(last_key.is_none() || last_key.unwrap() < this_key);
            last_key = Some(this_key);

            match cursor.page()?.node {
                Node::Leaf(ref lnode) => {
                    let mut last_key = None;
                    for cell in &lnode.cells {
                        assert!(last_key.is_none() || last_key.unwrap() < cell.key);
                        last_key = Some(cell.key);
                    }
                }
                Node::Internal(ref inode) => {
                    let mut last_key = None;
                    for child in &inode.children {
                        assert!(last_key.is_none() || last_key.unwrap() < child.key);
                        last_key = Some(child.key);
                    }
                    assert_eq!(
                        inode.right_child.unwrap(),
                        inode.children.last().unwrap().child
                    );
                }
            }

            cursor.advance()?;
        }

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

    fn run(&mut self, bail_on_fail: bool) -> TableResult<i32> {
        loop {
            write!(&mut self.output, "db > ")?;
            self.output.flush()?;
            let mut line = String::new();
            match self.input.read_line(&mut line) {
                Ok(_) => {
                    let line = line.trim();
                    writeln!(&mut self.output)?;

                    let c = if let Some(c) = line.chars().next() {
                        c
                    } else {
                        continue;
                    };

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
                                writeln!(
                                    &mut self.output,
                                    "InternalNode::MAX_CELLS: {}",
                                    &InternalNode::MAX_CELLS
                                )?;
                            }
                            ".exit" => return Ok(0),
                            _ => {
                                write!(&mut self.output, "Unknown command: {}", &line)?;
                            }
                        }
                    } else if let Some(stmt) = Statement::parse(line) {
                        if let Err(e) = self.table.exec(&stmt, &mut self.output) {
                            writeln!(&mut self.output, "Error: {:?}", &e)?;
                            if bail_on_fail {
                                return Err(e);
                            }
                        }
                    } else {
                        panic!("Unknown command: `{}`", line);
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
    repl.run(false)
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
    use rand::{Rng, SeedableRng};
    use std::collections::BTreeSet;
    use tempfile::tempfile;

    fn run_from_file<S: AsRef<str>>(file: File, input: &[S]) -> TableResult<Vec<String>> {
        let pager = Pager::from_file(file).unwrap();
        let mut input_bytes = Vec::new();
        for line in input {
            input_bytes.extend_from_slice(line.as_ref().as_bytes());
            input_bytes.push('\n' as u8);
        }
        let mut output = Vec::new();
        let mut repl = Repl::new(&input_bytes[..], &mut output, pager)?;
        repl.run(true)?;
        let output = String::from_utf8(output).unwrap();
        Ok(output
            .lines()
            .filter(|line| line != &"db > ")
            .map(|line| line.to_owned())
            .collect())
    }

    fn run<S: AsRef<str>>(input: &[S]) -> TableResult<Vec<String>> {
        run_from_file(tempfile().unwrap(), input)
    }

    #[test]
    fn empty() {
        assert_eq!(run(&["select", ".exit"]).unwrap(), ["0 rows."]);
    }

    #[test]
    fn echo() {
        assert_eq!(
            run(&["insert 11 john john@john.com", "select", ".exit"]).unwrap(),
            [" [00000011 'john' 'john@john.com']", "1 rows.",]
        );
    }

    fn persist_helper(keys: &[Id]) -> TableResult<()> {
        let tempdb = tempfile::NamedTempFile::new().unwrap();
        let inserts: Vec<_> = keys
            .iter()
            .map(|i| format!("insert {} john{} john{}@john.com", i, i, i))
            .collect();
        let expected = {
            let mut expected: Vec<_> = keys
                .iter()
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
            assert_eq!(
                run_from_file(tempdb.reopen().unwrap(), &commands)?,
                expected
            );
        }

        {
            let mut commands = Vec::new();
            commands.push("select".to_owned());
            commands.push(".exit".to_owned());
            assert_eq!(
                run_from_file(tempdb.reopen().unwrap(), &commands)?,
                expected
            );
        }

        Ok(())
    }

    fn persist_both_directions(max: Id) {
        let keys: Vec<Id> = (0..max).collect();
        persist_helper(&keys).unwrap();
        let keys: Vec<Id> = keys.iter().rev().cloned().collect();
        persist_helper(&keys).unwrap();
    }

    #[test]
    fn full_leaf() {
        persist_both_directions(LeafNode::MAX_CELLS as Id);
    }

    #[test]
    fn split_leaf() {
        persist_both_directions((LeafNode::MAX_CELLS + 1) as Id);
    }

    #[test]
    fn split_leaf_twice() {
        persist_both_directions((2 * LeafNode::MAX_CELLS + 1) as Id);
    }

    #[test]
    fn split_leaves_a_bunch() {
        persist_both_directions(5 * LeafNode::MAX_CELLS as Id);
    }

    #[test]
    fn split_leaves_a_bunch_randomly() {
        let key_count = 5 * LeafNode::MAX_CELLS;
        let mut keys: BTreeSet<Id> = BTreeSet::new();

        let seed = [0u8; 32];
        let mut rng = rand::rngs::StdRng::from_seed(seed);
        for _ in 0..key_count {
            let key: Id = rng.gen();
            keys.insert(key % 100000000);
        }
        let keys: Vec<_> = keys.iter().cloned().collect();
        persist_helper(&keys).unwrap();
    }

    #[test]
    fn fill_first_level() {
        let max = InternalNode::MAX_CELLS * LeafNode::MAX_CELLS;
        let keys: Vec<Id> = (0..max as Id).collect();
        assert!(match persist_helper(&keys) {
            Err(TableError::CannotSplitInternalNodes) => true,
            _ => false,
        });
        let keys: Vec<Id> = keys.iter().rev().cloned().collect();
        assert!(match persist_helper(&keys) {
            Err(TableError::CannotSplitInternalNodes) => true,
            _ => false,
        });
    }

    #[test]
    fn duplicate() {
        assert!(match run(&[
            "insert 11 john11 john11@john.com",
            "insert 11 john11 john11@john.com",
            "select",
            ".exit"
        ]) {
            Err(TableError::IdAlreadyExists) => true,
            _ => false,
        });
    }

    #[test]
    fn persist() {
        let keys: Vec<Id> = (1..=11).collect();
        persist_helper(&keys).unwrap();
    }
}
