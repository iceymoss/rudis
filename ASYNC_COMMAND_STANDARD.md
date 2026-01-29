# 异步命令标准设计文档

## 概述

本文档定义了 Rudis 中异步命令（需要 Handler 上下文的命令）的标准实现方式。通过统一的 trait 接口和模块化设计，确保异步命令的实现既符合项目规范，又易于扩展和维护。

## 设计目标

1. **统一接口**：所有异步命令使用相同的 trait 接口
2. **降低耦合**：命令逻辑集中在命令文件中，不依赖 `server.rs`
3. **易于扩展**：新增异步命令只需实现 trait 并添加一个 match 分支
4. **代码清晰**：`server.rs` 保持简洁，只负责调度，不包含业务逻辑

## 架构设计

### 1. Trait 定义：`HandlerAsyncCommand`

位置：`src/cmds/traits.rs`

```rust
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
```

**设计原则**：
- 所有需要 Handler 的异步命令都实现这个 trait
- 命令自己负责所有业务逻辑（阻塞、超时、唤醒等）
- Handler 只提供资源访问，不包含业务逻辑

### 2. 命令实现示例：`BLPOP`

位置：`src/cmds/listing/blpop.rs`

```rust
#[derive(Clone)]
pub struct Blpop {
    keys: Vec<String>,
    timeout: u64,
}

impl HandlerAsyncCommand for Blpop {
    fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
        // 解析逻辑
    }
    
    async fn apply(self, handler: &mut Handler) -> Result<Frame, Error> {
        // 1. 先尝试非阻塞获取
        // 2. 如果为空，注册阻塞请求
        // 3. 使用 tokio::select! 等待结果或超时
        // 所有逻辑都在这里，不依赖 server.rs
    }
}
```

**关键点**：
- 所有阻塞逻辑都在 `apply` 方法中
- 使用 `handler.get_blocking_manager()` 访问阻塞管理器
- 使用 `handler.apply_db_command()` 执行数据库操作
- 使用 `tokio::select!` 处理超时

### 3. 统一调度入口：`command_handler.rs`

位置：`src/server/command_handler.rs`

```rust
pub async fn try_apply_command(
    handler: &mut Handler,
    command: &Command,
) -> Option<Result<Frame, Error>> {
    match command {
        // 纯异步阻塞命令：通过 HandlerAsyncCommand trait 调用
        Command::Blpop(blpop) => {
            Some(blpop.clone().apply(handler).await)
        }
        Command::Brpop(brpop) => {
            Some(brpop.clone().apply(handler).await)
        }
        
        // 需要阻塞检查的写命令：先尝试唤醒阻塞客户端，再走 Db
        Command::Lpush(_) | Command::Rpush(_) => {
            Some(handle_blocking_aware_command(handler, command.clone()).await)
        }
        
        // 其他命令：不在这里处理，返回 None 让调用者按普通命令处理
        _ => None,
    }
}
```

**设计原则**：
- 所有"哪些命令需要 Handler"的判断都集中在这里
- 如果命令不需要 Handler，返回 `None`，让调用者按普通命令处理
- 新增异步命令时，只需在这里添加一个 match 分支

### 4. `server.rs` 集成

位置：`src/server.rs`

```rust
impl Handler {
    async fn apply_command(&mut self, command: Command) -> Result<Frame, Error> {
        // 先尝试异步命令调度器
        if let Some(result) = try_apply_command(self, &command).await {
            return result;
        }

        // 其他命令按原有逻辑走 Db
        match command {
            Command::Auth(auth) => auth.apply(self),
            Command::Client(client) => client.apply(),
            // ... 其他命令 ...
            _ => self.apply_db_command(command).await,
        }
    }
}
```

**关键点**：
- `server.rs` 不再包含任何异步命令的业务细节
- 只需要调用 `try_apply_command` 即可
- 如果返回 `None`，按普通命令处理

## 实现流程

### 新增异步命令的步骤

1. **在 `cmds` 目录下实现命令结构体**
   ```rust
   #[derive(Clone)]
   pub struct MyAsyncCommand {
       // 字段
   }
   ```

2. **实现 `HandlerAsyncCommand` trait**
   ```rust
   impl HandlerAsyncCommand for MyAsyncCommand {
       fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
           // 解析逻辑
       }
       
       async fn apply(self, handler: &mut Handler) -> Result<Frame, Error> {
           // 所有业务逻辑都在这里
       }
   }
   ```

3. **在 `command.rs` 中添加枚举分支**
   ```rust
   pub enum Command {
       // ...
       MyAsyncCommand(MyAsyncCommand),
   }
   
   impl Command {
       pub fn parse_from_frame(frame: Frame) -> Result<Self, Error> {
           match command_name.to_uppercase().as_str() {
               // ...
               "MYASYNC" => Command::MyAsyncCommand(MyAsyncCommand::parse_from_frame(frame)?),
               // ...
           }
       }
   }
   ```

4. **在 `command_handler.rs` 中添加调度分支**
   ```rust
   pub async fn try_apply_command(...) -> Option<Result<Frame, Error>> {
       match command {
           // ...
           Command::MyAsyncCommand(cmd) => {
               Some(cmd.clone().apply(handler).await)
           }
           // ...
       }
   }
   ```

## 当前实现

### 已实现的异步命令

1. **BLPOP** (`src/cmds/listing/blpop.rs`)
   - 阻塞式从列表左端弹出元素
   - 实现 `HandlerAsyncCommand` trait
   - 所有逻辑在 `apply` 方法中

2. **BRPOP** (`src/cmds/listing/brpop.rs`)
   - 阻塞式从列表右端弹出元素
   - 实现 `HandlerAsyncCommand` trait
   - 所有逻辑在 `apply` 方法中

### 阻塞感知命令

1. **LPUSH/RPUSH** (`src/cmds/listing/lpush.rs`, `rpush.rs`)
   - 普通命令，但需要检查是否有阻塞等待者
   - 在 `command_handler.rs::handle_blocking_aware_command` 中处理
   - 如果有等待者，直接唤醒；否则正常执行数据库操作

## 优势

1. **模块化**：每个命令的完整逻辑都在自己的文件中
2. **可扩展**：新增异步命令只需实现 trait 和添加一个 match 分支
3. **低耦合**：`server.rs` 不包含业务逻辑，只负责调度
4. **统一接口**：所有异步命令使用相同的 trait，代码风格一致
5. **易于测试**：每个命令可以独立测试

## 注意事项

1. **Clone 要求**：由于 `Command` 枚举需要 `Clone`，所有异步命令结构体必须实现 `Clone`
2. **资源访问**：通过 `handler` 的方法访问资源，不要直接访问字段
3. **锁管理**：在使用 `Mutex` 时，注意在 `await` 前释放锁，避免死锁
4. **错误处理**：所有错误都应该返回 `Result<Frame, Error>`

## 未来扩展

1. **更多异步命令**：可以按照这个标准实现其他需要 Handler 的异步命令
2. **Trait 优化**：可以考虑添加默认实现或辅助方法
3. **文档生成**：可以基于 trait 自动生成命令文档

## 总结

通过 `HandlerAsyncCommand` trait 和统一的调度入口，我们实现了：
- ✅ 异步命令的标准接口
- ✅ 命令逻辑的模块化
- ✅ `server.rs` 的最小入侵
- ✅ 易于扩展和维护的架构

这个设计为未来的异步命令实现提供了清晰的标准和最佳实践。
