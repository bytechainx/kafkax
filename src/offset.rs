//! 应用层 offset 提交存储。
//!
//! `rskafka` 没有 consumer group coordinator，因此 offset 由应用显式持久化（这是本库的
//! 核心价值之一，用来在无 group 的前提下实现 at-least-once）。
//!
//! - [`OffsetCommitStore::commit`]：把已处理消息的 `delivered_offset` 记为
//!   **next-to-read = offset + 1**
//! - [`OffsetCommitStore::committed`]：读取 next-to-read（无记录返回 `None`）
//!
//! 提交是**单调**的：较小的 offset 不会回退已提交位点。

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures_core::future::BoxFuture;
use tokio::sync::Mutex;

use crate::error::{KafkaError, KafkaResult};

/// Offset 提交存储抽象。
///
/// 存储的是 **下一次应读取的 offset**（Kafka group 语义中的 committed offset）。
///
/// 方法返回 [`BoxFuture`] 而非 `async fn`，以保持 trait 对象安全：
/// [`AtLeastOnceConsumer`](crate::AtLeastOnceConsumer) 持有 `Arc<dyn OffsetCommitStore>`。
pub trait OffsetCommitStore: Send + Sync {
    /// 读取 `(topic, partition)` 已提交的 next-to-read offset。
    fn committed<'a>(
        &'a self,
        topic: &'a str,
        partition: i32,
    ) -> BoxFuture<'a, KafkaResult<Option<i64>>>;

    /// 提交已成功处理的消息 offset：内部写入 `next = offset + 1`。
    fn commit<'a>(
        &'a self,
        topic: &'a str,
        partition: i32,
        offset: i64,
    ) -> BoxFuture<'a, KafkaResult<()>>;
}

/// 内存 offset 表（进程内；单实例与测试默认实现）。
#[derive(Debug, Default)]
pub struct MemoryOffsetStore {
    inner: Mutex<HashMap<(String, i32), i64>>,
}

impl MemoryOffsetStore {
    /// 新建空表。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 包装为 `Arc`。
    #[must_use]
    pub fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }

    /// 直接写入 next-to-read（单调；测试与位点迁移辅助）。
    ///
    /// # Errors
    ///
    /// `next_offset` 为负时返回 [`KafkaError::Config`]。
    pub async fn put_next(&self, topic: &str, partition: i32, next_offset: i64) -> KafkaResult<()> {
        if next_offset < 0 {
            return Err(KafkaError::Config("next offset 不能为负".into()));
        }
        let mut guard = self.inner.lock().await;
        let entry = guard
            .entry((topic.to_string(), partition))
            .or_insert(next_offset);
        *entry = (*entry).max(next_offset);
        Ok(())
    }
}

impl OffsetCommitStore for MemoryOffsetStore {
    fn committed<'a>(
        &'a self,
        topic: &'a str,
        partition: i32,
    ) -> BoxFuture<'a, KafkaResult<Option<i64>>> {
        Box::pin(async move {
            let guard = self.inner.lock().await;
            Ok(guard.get(&(topic.to_string(), partition)).copied())
        })
    }

    fn commit<'a>(
        &'a self,
        topic: &'a str,
        partition: i32,
        offset: i64,
    ) -> BoxFuture<'a, KafkaResult<()>> {
        Box::pin(async move {
            let next = next_offset_of(offset)?;
            let mut guard = self.inner.lock().await;
            let entry = guard.entry((topic.to_string(), partition)).or_insert(next);
            *entry = (*entry).max(next);
            Ok(())
        })
    }
}

/// 文件持久化：每行 `topic<TAB>partition<TAB>next_offset`。
///
/// 采用「同目录临时文件 + `fsync` + `rename` + 父目录 `fsync`」的原子写入，避免
/// 进程在中途退出时留下半个文件。同一实例内用异步锁串行化读改写。
#[derive(Debug)]
pub struct FileOffsetStore {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl FileOffsetStore {
    /// 绑定文件路径（首次提交时按需创建父目录）。
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// 文件路径。
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 读取全部位点；文件不存在视为空表。
    fn load_map(path: &Path) -> KafkaResult<HashMap<(String, i32), i64>> {
        if !path.exists() {
            return Ok(HashMap::new());
        }
        let text = std::fs::read_to_string(path).map_err(KafkaError::Io)?;
        let mut map = HashMap::new();
        for (index, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let lineno = index + 1;
            let mut parts = line.split('\t');
            let topic = parts
                .next()
                .ok_or_else(|| KafkaError::Config(format!("offset 行 {lineno}: 缺少 topic")))?;
            let partition = parse_field::<i32>(parts.next(), lineno, "partition")?;
            let next = parse_field::<i64>(parts.next(), lineno, "next_offset")?;
            map.insert((topic.to_string(), partition), next);
        }
        Ok(map)
    }

    /// 原子写回全部位点。
    fn save_map(path: &Path, map: &HashMap<(String, i32), i64>) -> KafkaResult<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(KafkaError::Io)?;
            }
        }
        let mut lines: Vec<String> = map
            .iter()
            .map(|((topic, partition), next)| format!("{topic}\t{partition}\t{next}"))
            .collect();
        lines.sort();
        let body = lines.join("\n");
        let tmp = path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)
            .map_err(KafkaError::Io)?;
        file.write_all(body.as_bytes()).map_err(KafkaError::Io)?;
        file.sync_all().map_err(KafkaError::Io)?;
        std::fs::rename(&tmp, path).map_err(KafkaError::Io)?;
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        OpenOptions::new()
            .read(true)
            .open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(KafkaError::Io)?;
        Ok(())
    }
}

impl OffsetCommitStore for FileOffsetStore {
    fn committed<'a>(
        &'a self,
        topic: &'a str,
        partition: i32,
    ) -> BoxFuture<'a, KafkaResult<Option<i64>>> {
        Box::pin(async move {
            let path = self.path.clone();
            let guard = Arc::clone(&self.lock).lock_owned().await;
            let topic = topic.to_string();
            tokio::task::spawn_blocking(move || {
                let _guard = guard;
                let map = Self::load_map(&path)?;
                Ok(map.get(&(topic, partition)).copied())
            })
            .await
            .map_err(|error| KafkaError::Io(std::io::Error::other(error.to_string())))?
        })
    }

    fn commit<'a>(
        &'a self,
        topic: &'a str,
        partition: i32,
        offset: i64,
    ) -> BoxFuture<'a, KafkaResult<()>> {
        Box::pin(async move {
            let next = next_offset_of(offset)?;
            let path = self.path.clone();
            let guard = Arc::clone(&self.lock).lock_owned().await;
            let topic = topic.to_string();
            tokio::task::spawn_blocking(move || {
                let _guard = guard;
                let mut map = Self::load_map(&path)?;
                let entry = map.entry((topic, partition)).or_insert(next);
                *entry = (*entry).max(next);
                Self::save_map(&path, &map)
            })
            .await
            .map_err(|error| KafkaError::Io(std::io::Error::other(error.to_string())))?
        })
    }
}

/// 计算 next-to-read；负值与溢出都显式失败，避免伪报成功。
fn next_offset_of(offset: i64) -> KafkaResult<i64> {
    if offset < 0 {
        return Err(KafkaError::Config("commit offset 不能为负".into()));
    }
    offset
        .checked_add(1)
        .ok_or_else(|| KafkaError::Config("commit offset 溢出 i64".into()))
}

/// 解析 TSV 字段。
fn parse_field<T>(value: Option<&str>, lineno: usize, name: &str) -> KafkaResult<T>
where
    T: std::str::FromStr,
{
    let raw =
        value.ok_or_else(|| KafkaError::Config(format!("offset 行 {lineno}: 缺少 {name}")))?;
    raw.trim()
        .parse()
        .map_err(|_| KafkaError::Config(format!("offset 行 {lineno}: {name} 非法")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_commit_advances_to_next_to_read() {
        let store = MemoryOffsetStore::new();
        assert!(store.committed("t", 0).await.expect("读取位点").is_none());
        store.commit("t", 0, 5).await.expect("提交位点");
        assert_eq!(store.committed("t", 0).await.expect("读取位点"), Some(6));
        store.commit("t", 1, 0).await.expect("提交分区一位点");
        assert_eq!(store.committed("t", 1).await.expect("读取位点"), Some(1));
        assert_eq!(store.committed("t", 0).await.expect("读取位点"), Some(6));
    }

    #[tokio::test]
    async fn memory_commit_is_monotonic_and_rejects_overflow() {
        let store = MemoryOffsetStore::new();
        store.put_next("t", 0, 11).await.expect("写入下一位点");
        store.put_next("t", 0, 4).await.expect("旧位点幂等");
        store.commit("t", 0, 3).await.expect("旧提交幂等");
        assert_eq!(store.committed("t", 0).await.expect("读取位点"), Some(11));

        assert!(store.commit("t", 0, -1).await.is_err());
        assert!(store.commit("t", 0, i64::MAX).await.is_err());
        assert!(store.put_next("t", 0, -1).await.is_err());
    }

    #[tokio::test]
    async fn file_store_roundtrip_is_atomic_and_monotonic() {
        let dir = std::env::temp_dir().join(format!("kafkax-offset-{}", std::process::id()));
        let cleanup = dir.clone();
        tokio::task::spawn_blocking(move || {
            let _ = std::fs::remove_dir_all(&cleanup);
            std::fs::create_dir_all(cleanup)
        })
        .await
        .expect("等待创建临时目录")
        .expect("创建临时目录");

        let path = dir.join("offsets.tsv");
        let store = FileOffsetStore::new(&path);
        store.commit("orders", 2, 99).await.expect("提交文件位点");
        assert_eq!(
            store.committed("orders", 2).await.expect("读取文件位点"),
            Some(100)
        );

        // 重新打开可读到，且落后提交不回退
        let reopened = FileOffsetStore::new(&path);
        assert_eq!(
            reopened.committed("orders", 2).await.expect("重开读取"),
            Some(100)
        );
        reopened.commit("orders", 2, 10).await.expect("旧位点幂等");
        assert_eq!(
            reopened.committed("orders", 2).await.expect("读取文件位点"),
            Some(100)
        );

        // 临时文件不残留
        assert!(!path.with_extension("tmp").exists());
        let cleanup = dir.clone();
        tokio::task::spawn_blocking(move || std::fs::remove_dir_all(cleanup))
            .await
            .expect("等待清理临时目录")
            .expect("清理临时目录");
    }

    #[test]
    fn next_offset_rejects_negative_and_overflow() {
        assert!(next_offset_of(-1).is_err());
        assert!(next_offset_of(i64::MAX).is_err());
        assert_eq!(next_offset_of(0).expect("合法"), 1);
    }
}
