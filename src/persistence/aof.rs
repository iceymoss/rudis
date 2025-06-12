use std::fs::File;
use std::io::Seek;
use std::io::{SeekFrom, Write};
use ahash::AHashMap;
use parking_lot::Mutex;
use std::{fs::OpenOptions, sync::Arc};

use indicatif::{ProgressBar, ProgressStyle};

use crate::db::db::Db;
use crate::command_strategies::init_command_strategies;
use crate::db::db_config::RudisConfig;
use crate::session::session::Session;

// aof持久化就是将写命令和参数追加到文件中，恢复数据时，就是从持久化文件中一次将命令执行一遍
// aof本身是命令集，无需考虑库号，默认是select 0 的
// 当用户发送了select 1这个命令本身也会被记录，在恢复时，执行已经跟着这个select 1进行切换了
pub struct Aof {
    pub rudis_config: Arc<RudisConfig>,
    pub db: Arc<Mutex<Db>>,
    pub aof_file: Option<std::fs::File>,
}

impl Aof {
    
    pub fn new(rudis_config: Arc<RudisConfig>, db: Arc<Mutex<Db>>) -> Aof {
        let mut aof_file = None;
        if rudis_config.appendonly && rudis_config.appendfilename.is_some() {
            if let Some(filename) = &rudis_config.appendfilename {
                let base_path = &rudis_config.dir;
                let file_path = format!("{}{}", base_path, filename);
                aof_file = Some(OpenOptions::new().create(true).read(true).append(true).open(file_path).expect("Failed to open AOF file"));
            }
        }
        Aof {
            rudis_config,
            db,
            aof_file,
        }
    }

    /*
     * 写入 aof 日志【增量】
     *
     * @param command 命令
     */
    pub fn save(&mut self, command: &str) {
        if let Some(file) = self.aof_file.as_mut() {
            if let Err(err) = writeln!(file, "{}", command) {
                eprintln!("Failed to append to AOF file: {}", err);
            }
        }
    }

    /*
     * 解析 appendfile 文件，执行命令，回填数据
     *
     * 调用时机：项目启动
     */
    // aof文件写入内存db中
    pub fn load(&mut self) {
        if self.rudis_config.appendonly {
            if let Some(filename) = &self.rudis_config.appendfilename {
                let base_path = &self.rudis_config.dir;
                let file_path = format!("{}{}", base_path, filename);
                if let Ok(mut file) = File::open(file_path) {
                    use std::io::{BufRead, BufReader};
                    let line_count = BufReader::new(&file).lines().count() as u64;
                    
                    // 获取命令处理器集
                    let command_strategies = init_command_strategies();
                    
                    // 实例化会话
                    let sessions: Arc<Mutex<AHashMap<String, Session>>> = Arc::new(Mutex::new(AHashMap::new()));
                    let session_id = "0.0.0.0:0";

                    {
                        let mut sessions_ref = sessions.lock();
                        let mut session = Session::new();
                        session.set_selected_database(0);
                        session.set_authenticated(true);
                        sessions_ref.insert(session_id.to_string(), session);
                    }

                    // 移动读取指针到首行
                    if file.seek(SeekFrom::Start(0)).is_ok() {
                        // 实例化进度条
                        let pb = ProgressBar::new(line_count);
                        
                        // 设置进度条样式
                        pb.set_style(ProgressStyle::default_bar().template("[{bar:39.green/cyan}] percent: {percent}% lines: {pos}/{len}").progress_chars("=>-"));
                        
                        // 读取内容
                        let reader = BufReader::new(&mut file);
                        
                        // 按行处理
                        for line in reader.lines() {
                            if let Ok(operation) = line {
                                
                                // 解析命令
                                let fragments: Vec<&str> = operation.split("\\r\\n").collect();
                                let command = fragments[2];
                                if let Some(strategy) = command_strategies.get(command.to_uppercase().as_str()) {
                                    strategy.execute(None, &fragments, &self.db, &self.rudis_config, &sessions,session_id);
                                }
                            }
                            pb.inc(1);
                        }
                        pb.finish();
                    }
                }
            }
        }
    }
}
