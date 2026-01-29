use std::sync::Arc;
use tokio::sync::Mutex;
use crate::store::blocking::BlockingQueueManager;

/// 创建并初始化阻塞管理器
pub fn create_blocking_manager() -> Arc<Mutex<BlockingQueueManager>> {
    let blocking_manager = Arc::new(Mutex::new(BlockingQueueManager::new()));
    
    // 启动超时清理任务
    let blocking_manager_clone = blocking_manager.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let mut manager = blocking_manager_clone.lock().await;
            manager.cleanup_timeout_requests();
        }
    });
    
    blocking_manager
}

