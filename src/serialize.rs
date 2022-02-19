use std::{io::{Write, Read}, mem::size_of};

use crate::*;

pub trait Serialize where Self: Sized {
    fn serialize<W: Write>(&self, writer: W) -> TableResult<()>;
    fn deserialize<R: Read>(reader: R) -> TableResult<Self>;
}

impl Serialize for Row {
    fn serialize<W: Write>(&self, mut w: W) -> TableResult<()> {
        w.write_all(&self.id.to_le_bytes())?;
        w.write_all(&self.username)?;
        w.write_all(&self.email)?;
        Ok(())
    }

    fn deserialize<R: Read>(mut r: R) -> TableResult<Row> {
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
}

impl Serialize for LeafCell {
    fn deserialize<R: Read>(mut r: R) -> TableResult<LeafCell> {
        let mut id = [0u8; size_of::<Id>()];
        r.read_exact(&mut id)?;
        let row = Row::deserialize(r)?;
        Ok(LeafCell {
            key: Id::from_le_bytes(id),
            row,
        })
    }

    fn serialize<W: Write>(&self, mut w: W) -> TableResult<()> {
        w.write_all(&self.key.to_le_bytes())?;
        self.row.serialize(w)
    }
}

impl Serialize for LeafNode {
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

        for _ in self.cells.len()..Self::MAX_CELLS {
            LeafCell::default().serialize(&mut writer)?
        }

        writer.write_all(&[0u8; Self::WASTED_SPACE])?;
        Ok(())
    }

    fn deserialize<R: Read>(mut reader: R) -> TableResult<Self> {
        let mut cell_count = [0u8; size_of::<u32>()];
        reader.read_exact(&mut cell_count[..])?;
        let cell_count = u32::from_le_bytes(cell_count);

        let mut next_leaf = [0u8; size_of::<u32>()];
        reader.read_exact(&mut next_leaf[..])?;
        let next_leaf = u32::from_le_bytes(next_leaf);

        let mut node = Self::create_empty();
        for _ in 0..cell_count {
            node.cells.push(LeafCell::deserialize(&mut reader)?);
        }
        for _ in cell_count as usize..Self::MAX_CELLS {
            LeafCell::deserialize(&mut reader)?;
        }
        node.next_leaf = next_leaf;

        let mut padding = [0u8; Self::WASTED_SPACE];
        reader.read_exact(&mut padding)?;

        Ok(node)
    }
}

impl Serialize for InternalCell {
    fn deserialize<R: Read>(mut r: R) -> TableResult<InternalCell> {
        let mut id = [0u8; size_of::<Id>()];
        r.read_exact(&mut id)?;
        let mut page_num = [0u8; size_of::<PageNumber>()];
        r.read_exact(&mut page_num)?;
        Ok(InternalCell {
            key: Id::from_le_bytes(id),
            child: PageNumber::from_le_bytes(page_num),
        })
    }

    fn serialize<W: Write>(&self, mut w: W) -> TableResult<()> {
        w.write_all(&self.key.to_le_bytes())?;
        w.write_all(&self.child.to_le_bytes())?;
        Ok(())
    }
}

impl Serialize for InternalNode {
    fn serialize<W: Write>(&self, mut writer: W) -> TableResult<()> {
        let mut child_count = [0u8; size_of::<u32>()];
        child_count[0..4].copy_from_slice(&(self.children.len() as u32).to_le_bytes());
        writer.write_all(&child_count)?;

        for child in self.children.iter() {
            child.serialize(&mut writer)?;
        }

        for _ in self.children.len()..Self::MAX_CELLS {
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
        for _ in 0..child_count {
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