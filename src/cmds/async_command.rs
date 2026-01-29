use anyhow::Error;
use crate::frame::Frame;
use crate::store::db::Db;
use crate::server::Handler;

/// 命令执行上下文
/// 
/// 提供命令执行所需的所有资源，统一接口，降低耦合
/// 
/// 设计原则：
/// - 命令不直接依赖 `Db` 或 `Handler`，而是依赖 `CommandContext`
/// - 命令自己决定需要什么资源，通过 `get_db()` 或 `get_handler()` 获取
/// - 这样设计可以让命令实现更统一，降低耦合
/// 
/// **注意**：当前项目中，`CommandContext` 和 `Command` trait 尚未接入实际代码，
/// 仅作为未来统一命令接口的设计预留。当前真正使用的是 `HandlerAsyncCommand`。
pub struct CommandContext<'a> {
    /// 数据库实例（用于普通命令）
    db: Option<&'a mut Db>,
    
    /// Handler 实例（用于需要 Handler 上下文的命令）
    handler: Option<&'a mut Handler>,
}

impl<'a> CommandContext<'a> {
    /// 创建数据库上下文（用于普通命令）
    pub fn db(db: &'a mut Db) -> Self {
        Self {
            db: Some(db),
            handler: None,
        }
    }
    
    /// 创建 Handler 上下文（用于需要 Handler 的命令）
    pub fn handler(handler: &'a mut Handler) -> Self {
        Self {
            db: None,
            handler: Some(handler),
        }
    }
    
    /// 获取数据库实例（如果可用）
    /// 
    /// 普通命令使用这个方法获取数据库实例
    pub fn get_db(&mut self) -> Result<&mut Db, Error> {
        self.db.as_deref_mut()
            .ok_or_else(|| Error::msg("Database context not available"))
    }
    
    /// 获取 Handler 实例（如果可用）
    /// 
    /// 需要 Handler 上下文的命令使用这个方法获取 Handler 实例
    pub fn get_handler(&mut self) -> Result<&mut Handler, Error> {
        self.handler.as_deref_mut()
            .ok_or_else(|| Error::msg("Handler context not available"))
    }
    
    /// 检查是否有数据库上下文
    pub fn has_db(&self) -> bool {
        self.db.is_some()
    }
    
    /// 检查是否有 Handler 上下文
    pub fn has_handler(&self) -> bool {
        self.handler.is_some()
    }
}

/// 命令 trait：所有命令的统一接口
/// 
/// 这个 trait 定义了命令的标准接口：
/// 1. `parse_from_frame`: 从 RESP 帧解析命令
/// 2. `apply`: 执行命令，接收统一的 `CommandContext`
/// 
/// 通过这个 trait，所有命令（无论是普通命令还是需要 Handler 的命令）
/// 都可以使用相同的接口，降低耦合，提高可扩展性
/// 
/// **注意**：当前项目中，此 trait 尚未接入实际代码，仅作为未来统一命令接口的设计预留。
/// 当前真正使用的是 `HandlerAsyncCommand`。
/// 
/// 使用示例：
/// ```rust
/// impl Command for Set {
///     fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
///         // 解析逻辑
///     }
///     
///     fn apply(self, ctx: CommandContext<'_>) -> Result<Frame, Error> {
///         let db = ctx.get_db()?;
///         db.insert(self.key, Structure::String(self.val));
///         Ok(Frame::Ok)
///     }
/// }
/// ```
pub trait Command: Sized + Clone {
    /// 从 RESP 帧解析命令
    fn parse_from_frame(frame: Frame) -> Result<Self, Error>;
    
    /// 执行命令
    /// 
    /// 接收统一的 `CommandContext`，命令自己决定使用哪个资源
    /// - 普通命令：使用 `ctx.get_db()?`
    /// - 需要 Handler 的命令：使用 `ctx.get_handler()?`
    fn apply(self, ctx: CommandContext<'_>) -> Result<Frame, Error>;
}

/// Handler 异步命令 trait：专门用于需要 Handler 上下文的异步命令
/// 
/// **这是当前项目中真正使用的异步命令标准接口**，专门用于需要访问 Handler 资源的命令
/// （如 blocking_manager、session_manager）
/// 
/// **设计原则**：
/// - 所有需要 Handler 的异步命令都实现这个 trait
/// - 命令自己负责所有业务逻辑（阻塞、超时、唤醒等）
/// - Handler 只提供资源访问，不包含业务逻辑
/// 
/// **使用示例**：
/// ```rust
/// impl HandlerAsyncCommand for Blpop {
///     fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
///         // 解析逻辑
///     }
///     
///     async fn apply(self, handler: &mut Handler) -> Result<Frame, Error> {
///         // 1. 先尝试非阻塞获取
///         // 2. 如果为空，注册阻塞请求
///         // 3. 使用 tokio::select! 等待结果或超时
///         // 所有逻辑都在这里，不依赖 server.rs
///     }
/// }
/// ```
#[allow(async_fn_in_trait)]
pub trait HandlerAsyncCommand: Sized + Clone {
    /// 从 RESP 帧解析命令
    fn parse_from_frame(frame: Frame) -> Result<Self, Error>;
    
    /// 在 Handler 中异步执行命令
    /// 
    /// 接收 `&mut Handler`，命令自己决定如何使用 Handler 的资源
    /// - 访问 `blocking_manager`：`handler.get_blocking_manager()`
    /// - 访问 `session_manager`：`handler.get_session_manager()`
    /// - 执行数据库命令：`handler.apply_db_command(command).await`
    /// 
    /// 所有阻塞、超时、唤醒等逻辑都应该在这个方法内部完成
    async fn apply(self, handler: &mut Handler) -> Result<Frame, Error>;
}
