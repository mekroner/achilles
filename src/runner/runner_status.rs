use std::{io, process::Child};

pub struct RunnerStatus {
    pub coordinator_status: ProcessStatus,
    pub worker_status: Vec<ProcessStatus>,
}

impl RunnerStatus {
    pub fn all_running(&self) -> bool {
        !std::iter::once(&self.coordinator_status)
            .chain(self.worker_status.iter())
            .any(|state| *state != ProcessStatus::Running)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessStatus {
    Running,
    Success,
    Error(String),
}

impl std::fmt::Display for ProcessStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessStatus::Running => write!(f, "Running"),
            ProcessStatus::Success => write!(f, "Success"),
            ProcessStatus::Error(err) => write!(f, "Error({err})"),
        }
    }
}

impl TryFrom<&mut Child> for ProcessStatus {
    type Error = io::Error;
    fn try_from(
        child_process: &mut Child,
    ) -> Result<Self, <ProcessStatus as TryFrom<&mut Child>>::Error> {
        let Some(status) = child_process.try_wait()? else {
            log::trace!("Process is running.");
            return Ok(ProcessStatus::Running);
        };
        log::trace!("Coordinator has exited with {}", status);
        if status.success() {
            return Ok(ProcessStatus::Success);
        }
        Ok(ProcessStatus::Error(format!("{status}")))
    }
}
