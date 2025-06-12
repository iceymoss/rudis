use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use ahash::AHashMap;
use parking_lot::Mutex;
use crate::db::db::Db;
use crate::db::db_config::RudisConfig;
use crate::interface::command_type::CommandType;
use crate::session::session::Session;
use crate::tools::resp::RespValue;
use crate::interface::command_strategy::CommandStrategy;

pub struct HgetAllCommand {}

impl CommandStrategy for HgetAllCommand {
    fn execute(
        &self,
        stream: Option<&mut TcpStream>,
        fragments: &[&str],
        db: &Arc<Mutex<Db>>,
        _rudis_config: &Arc<RudisConfig>,
        sessions: &Arc<Mutex<AHashMap<String, Session>>>,
        session_id: &str
    ) {
        // hgetall userinfo
        let mut db_ref = db.lock();

        // 获取db编号
        let db_index = {
            let sessions_ref = sessions.lock();
            if let Some(session) = sessions_ref.get(session_id) {
                session.get_selected_database()
            } else {
                return;
            }
        };

        // 获取key
        let key = fragments[4].to_string();

        // 手动触发过期检查
        db_ref.check_ttl(db_index, &key);

        // 获取数据
        match db_ref.hgetall(db_index, &key) {
            Some(values) => {
                // 创建 RESP 数组，包含交替的字段和值
                let resp_array = RespValue::Array(
                    values.iter()
                        .flat_map(|(field, value)| {
                            vec![
                                RespValue::BulkString(field.to_string()),
                                RespValue::BulkString(value.to_string())
                            ]
                        })
                        .collect()
                );

                if let Some(stream) = stream {
                    let response_bytes = resp_array.to_bytes();
                    if let Err(e) = stream.write_all(&response_bytes) {
                        eprintln!("Failed to write to stream: {}", e);
                    }
                }
            }
            None => {
                // 键不存在 - 返回空数组
                let empty_array = RespValue::Array(Vec::new());
                if let Some(stream) = stream {
                    if let Err(e) = stream.write_all(&empty_array.to_bytes()) {
                        eprintln!("Failed to write empty response to stream: {}", e);
                    }
                }
            }
        }
        
    }

    fn command_type(&self) -> crate::interface::command_type::CommandType {
        CommandType::Read
    }
}
