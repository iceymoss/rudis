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

位置：`src/cmds/async_command.rs`

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

> 说明：
> - 当前项目中还预留了一个更通用的统一接口（`CommandContext` + `Command` trait），用于未来把“普通命令”和“需要 Handler 的命令”统一到同一套接口上。
> - 但**当前实际接入并使用**的是本文档描述的 `HandlerAsyncCommand`（以及 `command_handler.rs` 的统一调度入口）。

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

### 阻塞列表的配套组件（BlockingQueueManager）

位置：`src/store/blocking.rs`

阻塞能力由独立的阻塞队列管理器提供（不放在 `server.rs` 里），核心思想是：
- **等待队列按 key 组织**：`key -> FIFO 等待队列`
- **等待者通过 oneshot 收到结果**：写命令触发唤醒时向等待者发送响应帧
- **支持断连清理与超时清理**：避免资源泄漏

关键结构（简述）：
- `BlockDirection { Left, Right }`：区分 BLPOP / BRPOP
- `BlockingRequest`：记录 `session_id`、`key`、`direction`、`timeout`、`response_sender`、`created_at`
- `BlockingQueueManager`：
  - `register_blocking_request(...) -> oneshot::Receiver<Frame>`
  - `try_wakeup(key, direction, value) -> Option<(session_id, Frame)>`
  - `cleanup_session(session_id)`
  - `cleanup_timeout_requests()`

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

### 扩展示例：RPOPLPUSH / BRPOPLPUSH（设计思路）

#### 1) RPOPLPUSH（普通命令）

语义：
- 从 `source` 列表右侧弹出一个元素
- 将该元素从左侧推入 `destination`
- 返回该元素；若 `source` 为空返回 `Null`

实现建议：
- 作为普通命令放在 `src/cmds/listing/rpoplpush.rs`
- 复用现有 list 结构与类型检查逻辑，保证 WRONGTYPE 行为一致

#### 2) BRPOPLPUSH（异步命令，推荐实现为 HandlerAsyncCommand）

语义：
- 若 `source` 非空，行为与 `RPOPLPUSH` 相同
- 若 `source` 为空，则阻塞等待直到有元素或超时；超时返回 `Null`

实现建议（与 BLPOP/BRPOP 一致的模式）：
- 放在 `src/cmds/listing/brpoplpush.rs` 并实现 `HandlerAsyncCommand`
- `apply` 内部流程：
  - 先尝试一次非阻塞的 `RPOPLPUSH`（可通过构造 `Frame` -> `Command` -> `handler.apply_db_command(...)` 复用现有执行路径）
  - 若为空则注册阻塞请求（在 `BlockingQueueManager` 上针对 `source` 注册 `Right` 方向等待）
  - `tokio::select!` 等待 `receiver` 或 `sleep(timeout)`
  - 被唤醒后再完成 “push 到 destination” 的数据库更新（保持语义清晰，减少对阻塞管理器的侵入）

> 备注：
> - 更严格的 Redis 兼容实现可以支持多 key 的阻塞与唤醒，但当前阻塞管理器可能做了“只处理第一个 key”的简化，这部分可作为后续增强点。

### 扩展示例：Pub/Sub（如何接入异步命令标准）

Pub/Sub 的命令通常需要会话上下文（订阅模式、向同一连接持续推送消息），因此非常适合使用 `HandlerAsyncCommand`：
- `SUBSCRIBE` / `UNSUBSCRIBE`
- `PSUBSCRIBE` / `PUNSUBSCRIBE`
- `PUBLISH`（也可走异步标准，统一入口）
- `PUBSUB`（查询订阅信息）

实现建议：
- 新增 `PubSubManager`（建议持有在 `Handler` 或 `Server` 上，例如 `Arc<Mutex<PubSubManager>>`），维护：
  - `channel -> subscribers`
  - `pattern -> subscribers`
  - `session_id -> subscriptions`（断连清理）
- `SUBSCRIBE` 的 `apply` 典型流程：
  - 通过 `handler` 获取当前 `session_id` 与连接写通道
  - 注册订阅到 `PubSubManager`
  - 让 Session 进入 “订阅模式”（`Handler` 的读循环需要在订阅模式下同时处理：客户端命令 + 订阅消息推送）
  - 发送 Redis 规范的订阅确认帧（`["subscribe", channel, count]`）
- `PUBLISH`：
  - 在 `PubSubManager` 内对订阅者广播 `["message", channel, payload]` / `["pmessage", pattern, channel, payload]`
  - 返回接收者数量

接入点：
- 在 `Command` 枚举与 `parse_from_frame` 添加命令分支
- 在 `src/server/command_handler.rs::try_apply_command` 为这些命令添加 match 分支（像 BLPOP/BRPOP 一样）

## 总结

通过 `HandlerAsyncCommand` trait 和统一的调度入口，我们实现了：
- ✅ 异步命令的标准接口
- ✅ 命令逻辑的模块化
- ✅ `server.rs` 的最小入侵
- ✅ 易于扩展和维护的架构

这个设计为未来的异步命令实现提供了清晰的标准和最佳实践。
