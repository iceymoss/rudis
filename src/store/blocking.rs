use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use crate::frame::Frame;

/// 阻塞方向
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockDirection {
    Left,   // BLPOP
    Right,  // BRPOP
}

/// 阻塞请求信息
pub struct BlockingRequest {
    pub session_id: usize,
    pub key: String,
    pub direction: BlockDirection,
    pub timeout: Option<Duration>,
    pub response_sender: oneshot::Sender<Frame>,
    pub created_at: Instant,
}

/// 阻塞队列管理器
/// 
/// 管理所有等待 BLPOP/BRPOP 的客户端请求
pub struct BlockingQueueManager {
    // key -> 等待该键的请求队列（FIFO）
    waiting_requests: HashMap<String, VecDeque<BlockingRequest>>,
    // session_id -> keys 映射（用于客户端断开时快速清理）
    session_to_keys: HashMap<usize, Vec<String>>,
}

impl BlockingQueueManager {
    pub fn new() -> Self {
        Self {
            waiting_requests: HashMap::new(),
            session_to_keys: HashMap::new(),
        }
    }
    
    /// 注册阻塞请求
    /// 
    /// 返回一个 receiver，用于等待结果
    pub fn register_blocking_request(
        &mut self,
        keys: Vec<String>,
        session_id: usize,
        direction: BlockDirection,
        timeout: Option<Duration>,
    ) -> oneshot::Receiver<Frame> {
        let (sender, receiver) = oneshot::channel();
        
        // 使用第一个键（简化实现，Redis 支持多键但这里先实现单键）
        let key = keys[0].clone();
        
        let request = BlockingRequest {
            session_id,
            key: key.clone(),
            direction,
            timeout,
            response_sender: sender,
            created_at: Instant::now(),
        };
        
        // 添加到等待队列（FIFO）
        self.waiting_requests
            .entry(key.clone())
            .or_insert_with(VecDeque::new)
            .push_back(request);
        
        // 记录 session 到 keys 的映射
        self.session_to_keys
            .entry(session_id)
            .or_insert_with(Vec::new)
            .push(key);
        
        receiver
    }
    
    /// 尝试唤醒等待的客户端
    /// 
    /// 当 LPUSH/RPUSH 执行时调用此方法
    /// 返回：
    /// - Some(Frame): 成功唤醒一个客户端，返回要发送给该客户端的结果
    /// - None: 没有等待的客户端
    pub fn try_wakeup(
        &mut self,
        key: &str,
        direction: BlockDirection,
        value: String,
    ) -> Option<(usize, Frame)> {
        if let Some(requests) = self.waiting_requests.get_mut(key) {
            // 查找匹配方向的第一个请求（FIFO）
            let mut found_index = None;
            for (i, req) in requests.iter().enumerate() {
                if req.direction == direction {
                    found_index = Some(i);
                    break;
                }
            }
            
            if let Some(index) = found_index {
                let request = requests.remove(index).unwrap();
                
                // 构建响应：Array[key, value]
                let response = Frame::Array(vec![
                    Frame::BulkString(key.to_string()),
                    Frame::BulkString(value),
                ]);
                
                // 发送结果给等待的客户端
                let _ = request.response_sender.send(response.clone());
                
                // 清理 session 映射
                if let Some(keys) = self.session_to_keys.get_mut(&request.session_id) {
                    keys.retain(|k| k != key);
                }
                
                // 如果队列为空，移除该键
                if requests.is_empty() {
                    self.waiting_requests.remove(key);
                }
                
                return Some((request.session_id, response));
            }
        }
        None
    }
    
    /// 检查是否有等待该键的客户端
    pub fn has_waiting(&self, key: &str, direction: BlockDirection) -> bool {
        if let Some(requests) = self.waiting_requests.get(key) {
            requests.iter().any(|req| req.direction == direction)
        } else {
            false
        }
    }
    
    /// 清理客户端的所有阻塞请求（客户端断开时调用）
    pub fn cleanup_session(&mut self, session_id: usize) {
        if let Some(keys) = self.session_to_keys.remove(&session_id) {
            for key in keys {
                if let Some(requests) = self.waiting_requests.get_mut(&key) {
                    // 收集需要清理的请求
                    let mut indices_to_remove = Vec::new();
                    for (i, req) in requests.iter().enumerate() {
                        if req.session_id == session_id {
                            indices_to_remove.push(i);
                        }
                    }
                    
                    // 从后往前移除，避免索引变化
                    for &i in indices_to_remove.iter().rev() {
                        if let Some(req) = requests.remove(i) {
                            let _ = req.response_sender.send(Frame::Null);
                        }
                    }
                    
                    if requests.is_empty() {
                        self.waiting_requests.remove(&key);
                    }
                }
            }
        }
    }
    
    /// 清理超时的请求
    /// 
    /// 这个方法应该定期调用（例如每秒一次）
    pub fn cleanup_timeout_requests(&mut self) {
        let now = Instant::now();
        let mut keys_to_remove = Vec::new();
        
        for (key, requests) in &mut self.waiting_requests {
            // 收集超时的请求索引
            let mut indices_to_remove = Vec::new();
            for (i, req) in requests.iter().enumerate() {
                if let Some(timeout) = req.timeout {
                    if now.duration_since(req.created_at) >= timeout {
                        indices_to_remove.push(i);
                    }
                }
            }
            
            // 从后往前移除，避免索引变化
            for &i in indices_to_remove.iter().rev() {
                if let Some(req) = requests.remove(i) {
                    let _ = req.response_sender.send(Frame::Null);
                }
            }
            
            if requests.is_empty() {
                keys_to_remove.push(key.clone());
            }
        }
        
        for key in keys_to_remove {
            self.waiting_requests.remove(&key);
        }
    }
}
