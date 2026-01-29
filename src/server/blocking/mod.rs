pub mod manager;
pub mod handler;

// 对外暴露统一的创建接口和唤醒接口，保持 server.rs 及其它模块的调用方式不变
pub use manager::create_blocking_manager;
pub use handler::try_wakeup_for_command;

