use std::sync::Arc;

use ahash::AHashMap;
use parking_lot::Mutex;

use crate::db::db_config::RudisConfig;

use super::session::Session;

pub struct SessionManager {
    // 会话id => Session {
    //    selected_database: 0,
    //    authenticated: false,
    // }
    sessions: Arc<Mutex<AHashMap<String, Session>>>,
    config: Arc<RudisConfig>,
}

impl SessionManager {

    // 构造函数，初始化 SessionManager
    pub fn new(config: Arc<RudisConfig>) -> Self {
        SessionManager {
            sessions: Arc::new(Mutex::new(AHashMap::new())),
            config,
        }
    }

    // 创建会话的方法
    pub fn create_session(&self, session_id: String) -> bool {
        // 将self.sessions上锁，并发安全
        let mut sessions_ref = self.sessions.lock();
        if self.config.maxclients == 0 || sessions_ref.len() < self.config.maxclients {
            sessions_ref.insert(session_id, Session::new());
            true
        } else {
            false
        }
    }

    // 安全认证的方法
    pub fn authenticate(&self, session_id: &str, command: &str) -> bool {
        // 将self.sessions上锁，并发安全
        let sessions_ref = self.sessions.lock();
        // 获取会话信息
        let session = sessions_ref.get(session_id).unwrap();
        
        // 检查是否非AUTH命令
        let is_not_auth_command = command.to_uppercase() != "AUTH";
        
        // 检查会话是否未认证
        let is_not_auth = !session.get_authenticated();

        // 如果服务器配置了密码
        // 并且客户端未认证
        // 执行命令为非auth命令
        if self.config.password.is_some() && is_not_auth && is_not_auth_command {
            false
        } else {
            true
        }
    }

    /*
     * 销毁会话
     *
     * @param session_id 会话编号
     */
    pub fn destroy_session(&self, session_id: &str) {
        let mut sessions_ref = self.sessions.lock();
        sessions_ref.remove(session_id);
    }

    // 返回一个会话的引用
    pub fn get_sessions(&self) -> Arc<Mutex<AHashMap<String, Session>>> {
        Arc::clone(&self.sessions)
    }
}