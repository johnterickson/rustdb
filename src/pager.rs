use crate::*;

pub struct Page {
    pub node: Node,
    pub parent: Option<PageNumber>,
}

impl Page {
    pub const HEADER_SIZE: usize = 6;

    pub fn create_leaf(parent: Option<PageNumber>) -> Page {
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

type PageSlot = Option<Box<Page>>;
type PageSlotLock = RwLock<PageSlot>;

pub type PageReadGuard<'a> = OwningRef<RwLockReadGuard<'a, PageSlot>, Page>;
pub type PageWriteGuard<'a> = OwningRefMut<RwLockWriteGuard<'a, PageSlot>, Page>;

pub type LeafNodeReadGuard<'a> = OwningRef<RwLockReadGuard<'a, PageSlot>, LeafNode>;
pub type LeafNodeWriteGuard<'a> = OwningRefMut<RwLockWriteGuard<'a, PageSlot>, LeafNode>;
pub type CellWriteGuard<'a> = OwningRefMut<RwLockWriteGuard<'a, PageSlot>, LeafCell>;

pub struct Pager {
    file: RwLock<File>,
    pages: Vec<PageSlotLock>,
}

impl Pager {
    pub fn from_file(file: File) -> TableResult<Pager> {
        let file_length = file.metadata()?.len();
        assert!(file_length % PAGE_SIZE as u64 == 0);
        let page_count = file_length / PAGE_SIZE as u64;
        let mut pages = Vec::new();
        for _ in 0..page_count {
            pages.push(RwLock::new(None));
        }
        Ok(Pager {
            file: RwLock::new(file),
            pages,
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> TableResult<Pager> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Pager::from_file(file)
    }

    pub fn alloc(&mut self, p: Page) -> TableResult<PageNumber> {
        let page_num = self.pages.len() as PageNumber;
        let file_offset = (PAGE_SIZE as u64) * (page_num as u64);
        let needed_file_length = file_offset + (PAGE_SIZE as u64);
        let file = self.file.write();
        file.set_len(needed_file_length)?;
        self.pages.push(RwLock::new(Some(Box::new(p))));
        Ok(page_num)
    }

    fn get_page_slot(&self, page_num: PageNumber) -> TableResult<RwLockReadGuard<'_, PageSlot>> {
        if page_num >= self.pages.len() as PageNumber {
            return Err(TableError::PageNotFound);
        }
        let page_slot = self.pages[page_num as usize].read();
        Ok(page_slot)
    }

    fn get_page_slot_mut(
        &self,
        page_num: PageNumber,
    ) -> TableResult<RwLockWriteGuard<'_, PageSlot>> {
        if page_num >= self.pages.len() as PageNumber {
            return Err(TableError::PageNotFound);
        }
        let page_slot = self.pages[page_num as usize].write();
        Ok(page_slot)
    }

    pub fn get_page(&self, page_num: PageNumber) -> TableResult<PageReadGuard> {
        loop {
            {
                let page_slot = self.get_page_slot(page_num)?;
                // try with a read lock
                if page_slot.is_some() {
                    let page_slot = OwningRef::new(page_slot);
                    let page = page_slot.map(|page_slot| page_slot.as_ref().unwrap().as_ref());
                    return Ok(page);
                }
            }

            // try to fault in an existing page
            let _ = self.get_page_mut(page_num)?;
        }
    }

    pub fn get_page_mut(&self, page_num: PageNumber) -> TableResult<PageWriteGuard> {
        let mut page_slot = self.get_page_slot_mut(page_num)?;

        let file_offset = (PAGE_SIZE as u64) * (page_num as u64);
        if page_slot.is_none() {
            let faulted_page = {
                let mut file = self.file.write();
                file.seek(SeekFrom::Start(file_offset))?;
                Page::deserialize(&mut *file)?
            };
            *page_slot = Some(Box::new(faulted_page));
        }

        let page_slot = OwningRefMut::new(page_slot);
        let page = page_slot.map_mut(|page_slot| page_slot.as_mut().unwrap().as_mut());
        Ok(page)
    }

    pub fn page_count(&self) -> PageNumber {
        self.pages.len() as PageNumber
    }

    fn flush_all(&mut self) -> TableResult<()> {
        for (page_num, page) in self.pages.iter().enumerate() {
            if let Some(page) = page.read().as_ref() {
                let file_offset = (PAGE_SIZE as u64) * (page_num as u64);
                let mut file = self.file.write();
                file.seek(SeekFrom::Start(file_offset))?;
                page.serialize(&mut *file)?;
            }
        }

        self.file.write().flush()?;
        Ok(())
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        self.flush_all().unwrap();
    }
}
