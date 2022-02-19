use crate::*;

pub trait ReadGuard<T>: Deref<Target = T> {}
impl<'a, T> ReadGuard<T> for RwLockReadGuard<'a, T> {}
impl<'a, T> ReadGuard<T> for RwLockUpgradableReadGuard<'a, T> {}
impl<'a, T> ReadGuard<T> for RwLockWriteGuard<'a, T> {}

pub trait WriteGuard<T>: ReadGuard<T> + DerefMut<Target = T> {}
impl<'a, T> WriteGuard<T> for RwLockWriteGuard<'a, T> {}

pub trait ReadUpgradeGuard<T>: ReadGuard<T> {
    type Write: WriteGuard<T>;
    fn upgrade_to_write(self) -> Self::Write;
}

impl<'a, T> ReadUpgradeGuard<T> for RwLockUpgradableReadGuard<'a, T> {
    type Write = RwLockWriteGuard<'a, T>;
    fn upgrade_to_write(self) -> Self::Write {
        RwLockUpgradableReadGuard::upgrade(self)
    }
}
