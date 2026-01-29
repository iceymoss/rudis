use anyhow::Error;
use crate::command::Command;
use crate::frame::Frame;
use crate::server::Handler;
use crate::cmds::async_command::HandlerAsyncCommand;

/// 统一的异步命令处理入口
/// 
/// **设计原则**：
/// - 只处理需要 Handler 上下文的异步命令（如 BLPOP/BRPOP）
/// - 所有"哪些命令需要 Handler"的判断都集中在这里
/// - 如果命令不需要 Handler，返回 None，让调用者按普通命令处理
/// 
/// **扩展方式**：
/// - 添加新的异步命令时，只需在这里添加一个 match 分支
/// - 调用命令的 `apply(handler)` 方法即可（通过 `HandlerAsyncCommand` trait）
pub async fn try_apply_command(
    handler: &mut Handler,
    command: &Command,
) -> Option<Result<Frame, Error>> {
    match command {
        // 纯异步阻塞命令：通过 HandlerAsyncCommand trait 调用
        Command::Blpop(blpop) => {
            Some(HandlerAsyncCommand::apply(blpop.clone(), handler).await)
        }
        Command::Brpop(brpop) => {
            Some(HandlerAsyncCommand::apply(brpop.clone(), handler).await)
        }

        // 需要阻塞检查的写命令：先尝试唤醒阻塞客户端，再走 Db
        Command::Lpush(lpush) => {
            Some(handle_blocking_aware_command(handler, Command::Lpush(lpush.clone())).await)
        }
        Command::Rpush(rpush) => {
            Some(handle_blocking_aware_command(handler, Command::Rpush(rpush.clone())).await)
        }

        // 其他命令：不在这里处理，返回 None 让调用者按普通命令处理
        _ => None,
    }
}

/// 统一处理需要阻塞检查的命令（当前是 LPUSH/RPUSH）
///
/// 如果命令有阻塞等待者，直接唤醒并转交数据（不存数据库）
/// 否则正常执行数据库操作
///
/// 这样设计的好处：
/// 1. Handler 保持简洁，不需要为每个命令添加 handle_xxx 方法
/// 2. 阻塞逻辑集中在 blocking_handler 模块
/// 3. 易于扩展：只需在 blocking_handler 中添加新命令的处理
async fn handle_blocking_aware_command(
    handler: &mut Handler,
    command: Command,
) -> Result<Frame, Error> {
    use crate::server::blocking::try_wakeup_for_command;

    // 尝试唤醒阻塞的客户端
    let wakeup_result = {
        let mut blocking_manager = handler.get_blocking_manager().lock().await;
        try_wakeup_for_command(&command, &mut blocking_manager)
    };

    if let Some((session_id, response_frame)) = wakeup_result {
        // 找到等待的 session，发送响应
        if let Some(session) = handler.get_session_manager().get_session(session_id) {
            session.connection.write_bytes(response_frame.as_bytes()).await;
        }

        // 返回成功（但不存数据库）
        return Ok(Frame::Integer(1));
    }

    // 没有等待者或唤醒失败，正常执行数据库操作
    handler.apply_db_command(command).await
}
