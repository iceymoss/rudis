use crate::command::Command;
use crate::frame::Frame;
use crate::store::blocking::{BlockDirection, BlockingQueueManager};

/// 尝试为命令唤醒阻塞的客户端
/// 
/// 如果命令是 LPUSH/RPUSH 且有关键的阻塞等待者，直接唤醒并返回结果
/// 否则返回 None，表示需要正常执行数据库操作
///
pub fn try_wakeup_for_command(
    command: &Command,
    blocking_manager: &mut BlockingQueueManager,
) -> Option<(usize, Frame)> {
    match command {
        Command::Lpush(lpush) => {
            let has_waiting = blocking_manager.has_waiting(lpush.key(), BlockDirection::Left);
            
            if has_waiting {
                if let Some(value) = lpush.values().first().cloned() {
                    if let Some((session_id, response_frame)) = blocking_manager.try_wakeup(
                        lpush.key(),
                        BlockDirection::Left,
                        value,
                    ) {
                        return Some((session_id, response_frame));
                    }
                }
            }
            None
        },
        Command::Rpush(rpush) => {
            let has_waiting = blocking_manager.has_waiting(rpush.key(), BlockDirection::Right);
            
            if has_waiting {
                if let Some(value) = rpush.values().first().cloned() {
                    if let Some((session_id, response_frame)) = blocking_manager.try_wakeup(
                        rpush.key(),
                        BlockDirection::Right,
                        value,
                    ) {
                        return Some((session_id, response_frame));
                    }
                }
            }
            None
        },
        _ => None,
    }
}

