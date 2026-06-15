/// DAGWorkflow — 有向无环图工作流 (替代 Python dag.py)
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct DAGWorkflow {
    tasks: HashMap<String, Vec<String>>,  // task_id -> [dependencies]
    status: HashMap<String, String>,      // task_id -> status
}

impl DAGWorkflow {
    pub fn new() -> Self {
        Self { tasks: HashMap::new(), status: HashMap::new() }
    }
    pub fn add_task(&mut self, id: &str, deps: Vec<String>) {
        self.tasks.insert(id.to_string(), deps);
        self.status.insert(id.to_string(), "pending".to_string());
    }
    pub fn ready_tasks(&self) -> Vec<String> {
        self.tasks.iter()
            .filter(|(id, deps)| {
                self.status.get(*id).map(|s| s == "pending").unwrap_or(false)
                    && deps.iter().all(|d| self.status.get(d).map(|s| s == "done").unwrap_or(false))
            })
            .map(|(id, _)| id.clone())
            .collect()
    }
    pub fn complete(&mut self, id: &str) {
        self.status.insert(id.to_string(), "done".to_string());
    }
}
