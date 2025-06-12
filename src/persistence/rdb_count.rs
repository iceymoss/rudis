// RdbCount RDB持久化计数器，每执行N次写操作触发一次RDB持久化
pub struct RdbCount {
    pub modify_statistics: u64,
}

impl RdbCount {

    pub fn new() -> RdbCount{

        RdbCount {
            modify_statistics: 0
        }
    }

    pub fn calc(&mut self) {
        self.modify_statistics += 1;
    }  

    pub fn init(&mut self) {
        self.modify_statistics = 0;
    }
}