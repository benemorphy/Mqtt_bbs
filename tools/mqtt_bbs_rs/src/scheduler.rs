/// BBScheduler — 定时任务调度 (替代 Python scheduler.py)
use tokio::time::{interval, Duration};

#[derive(Clone, Debug)]
pub struct ScheduledTask {
    pub id: String,
    pub task_type: String,
    pub interval_secs: u64,
    pub last_run: u64,
}

pub struct Scheduler {
    tasks: Vec<ScheduledTask>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self { tasks: Vec::new() }
    }
    pub fn add(&mut self, task: ScheduledTask) {
        self.tasks.push(task);
    }
    pub async fn start(&mut self) {
        let mut tick = interval(Duration::from_secs(10));
        loop {
            tick.tick().await;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
            for task in &mut self.tasks {
                if now - task.last_run >= task.interval_secs {
                    tracing::info!("[Scheduler] 触发: {}", task.id);
                    task.last_run = now;
                }
            }
        }
    }
}
