use std::thread;
use std::sync::Arc;
use parking_lot::Mutex;
use super::{rdb::Rdb, rdb_count::RdbCount};
use tokio::time::Duration;

// RdbScheduler rdb持久化调度器
pub struct RdbScheduler {
    pub rdb: Arc<Mutex<Rdb>>,
}

impl RdbScheduler {

    pub fn new(rdb: Arc<Mutex<Rdb>>) -> RdbScheduler {
        RdbScheduler {
            rdb,
        }
    }

    pub fn execute(&mut self, save: Vec<(u64,u64)>, arc_rdb_count: Arc<Mutex<RdbCount>>) {
        // 为每一个持久化策略启动一个子任务
        for (interval, count) in save {
            // 写操作计数器：写一次+1
            let rdb_count_clone = arc_rdb_count.clone();
            
            // 内存数据
            let rdb = Arc::clone(&self.rdb);
            
            // 启动子任务：做持久化处理
            tokio::spawn(async move {
                let duration = Duration::from_secs(interval);
                loop {
                    // 间隔休眠
                    thread::sleep(duration);
                    let mut rdb_guard = rdb.lock();
                    let mut rdb_count = rdb_count_clone.lock();
                    if rdb_count.modify_statistics >= count {
                        // 重置修改计数器
                        rdb_count.init();
                        
                        // 执行rdb快照
                        rdb_guard.save();
                    }
                }
            });
        }
    }
}