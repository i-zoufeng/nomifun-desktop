//! Crawl job lifecycle: create, start, pause, cancel, inspect.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use nomifun_api_types::WebSocketMessage;
use nomifun_common::{AppError, CrawlJobId, TimestampMs, UserId};
use nomifun_knowledge::service::KnowledgeService;
use nomifun_knowledge::source_url::HttpFetcher;
use nomifun_realtime::UserEventSink;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio_util::sync::CancellationToken;

use crate::claim;
use crate::error::CrawlError;
use crate::events::{CrawlEvent, CrawlEventSink};
use crate::executor::LocalExecutor;
use crate::fetcher::HttpCrawlFetcher;
use crate::frontier::{self, ScopeMatcher};
use crate::model::{
    CrawlJob, CrawlScope, CrawlSink, CrawlTask, DiscoveredUrl, JobProgress, JobStatus, RenderMode,
    TaskOutcome, TaskStatus,
};
use crate::politeness::{HttpRobotsSource, Politeness};
use crate::runner::{self, RunnerConfig};
use crate::sink::KnowledgeSink;
use crate::store;

/// Identifies our traffic to site operators, per the compliance boundary.
pub const CRAWLER_USER_AGENT: &str = "NomiFun-Crawler/1.0 (+https://www.nomifun.com)";

#[derive(Debug, Clone, Deserialize)]
pub struct NewJob {
    pub name: String,
    pub seeds: Vec<String>,
    #[serde(default)]
    pub scope: CrawlScope,
    #[serde(default = "default_depth")]
    pub max_depth: u32,
    #[serde(default = "default_max_urls")]
    pub max_urls: u32,
    #[serde(default)]
    pub render_mode: RenderMode,
    #[serde(default = "default_concurrency")]
    pub concurrency: u32,
    #[serde(default = "default_per_host")]
    pub per_host_concurrency: u32,
    #[serde(default = "default_delay")]
    pub delay_ms: u64,
    #[serde(default = "default_true")]
    pub respect_robots: bool,
    #[serde(default)]
    pub sink: CrawlSink,
}

fn default_depth() -> u32 {
    3
}
fn default_max_urls() -> u32 {
    10_000
}
fn default_concurrency() -> u32 {
    4
}
fn default_per_host() -> u32 {
    2
}
fn default_delay() -> u64 {
    500
}
fn default_true() -> bool {
    true
}

/// Job plus derived counters, as returned to the UI.
#[derive(Debug, Clone, Serialize)]
pub struct JobView {
    pub job_id: String,
    pub name: String,
    pub seeds: Vec<String>,
    pub scope: CrawlScope,
    pub max_depth: u32,
    pub max_urls: u32,
    pub render_mode: RenderMode,
    pub concurrency: u32,
    pub per_host_concurrency: u32,
    pub delay_ms: u64,
    pub respect_robots: bool,
    pub sink: CrawlSink,
    pub status: JobStatus,
    pub error_detail: Option<String>,
    pub started_at: Option<TimestampMs>,
    pub finished_at: Option<TimestampMs>,
    pub created_at: TimestampMs,
    pub progress: JobProgress,
}

impl JobView {
    fn new(job: CrawlJob, progress: JobProgress) -> Self {
        Self {
            job_id: job.job_id.to_string(),
            name: job.name,
            seeds: job.seeds,
            scope: job.scope,
            max_depth: job.max_depth,
            max_urls: job.max_urls,
            render_mode: job.render_mode,
            concurrency: job.concurrency,
            per_host_concurrency: job.per_host_concurrency,
            delay_ms: job.delay_ms,
            respect_robots: job.respect_robots,
            sink: job.sink,
            status: job.status,
            error_detail: job.error_detail,
            started_at: job.started_at,
            finished_at: job.finished_at,
            created_at: job.created_at,
            progress,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskView {
    pub task_id: String,
    pub url: String,
    pub host: String,
    pub depth: u32,
    pub status: TaskStatus,
    pub attempt_count: u32,
    pub http_status: Option<u16>,
    pub error_code: Option<String>,
    pub error_detail: Option<String>,
    pub completed_at: Option<TimestampMs>,
}

impl From<CrawlTask> for TaskView {
    fn from(t: CrawlTask) -> Self {
        Self {
            task_id: t.task_id.to_string(),
            url: t.url,
            host: t.host,
            depth: t.depth,
            status: t.status,
            attempt_count: t.attempt_count,
            http_status: t.http_status,
            error_code: t.error_code,
            error_detail: t.error_detail,
            completed_at: t.completed_at,
        }
    }
}

/// Broadcasts crawl events to the installation owner, mirroring the knowledge
/// domain's emitter.
pub struct RealtimeEvents {
    sink: Arc<dyn UserEventSink>,
    user_id: Arc<str>,
}

impl RealtimeEvents {
    pub fn new(sink: Arc<dyn UserEventSink>, user_id: Arc<str>) -> Self {
        Self { sink, user_id }
    }

    fn send(&self, name: &str, event: CrawlEvent) {
        match serde_json::to_value(&event) {
            Ok(value) => self
                .sink
                .send_to_user(&self.user_id, WebSocketMessage::new(name, value)),
            Err(err) => tracing::warn!(error = %err, "failed to serialize crawl event"),
        }
    }
}

impl CrawlEventSink for RealtimeEvents {
    fn job_progress(&self, job_id: &CrawlJobId, progress: &JobProgress) {
        self.send(
            "crawl.progress",
            CrawlEvent::Progress { job_id: job_id.to_string(), progress: progress.clone() },
        );
    }

    fn task_settled(&self, job_id: &CrawlJobId, task: &CrawlTask, outcome: &TaskOutcome) {
        self.send("crawl.task", CrawlEvent::from_outcome(job_id, task, outcome));
    }

    fn job_finished(&self, job_id: &CrawlJobId, status: JobStatus, progress: &JobProgress) {
        self.send(
            "crawl.finished",
            CrawlEvent::Finished {
                job_id: job_id.to_string(),
                status: status.as_str().to_string(),
                progress: progress.clone(),
            },
        );
    }
}

#[derive(Clone)]
struct RunningJob {
    run_id: u64,
    cancel: CancellationToken,
}

struct RunLease {
    job_id: String,
    run_id: u64,
    cancel: CancellationToken,
}

#[derive(Default)]
struct RunningJobs {
    entries: DashMap<String, RunningJob>,
    next_run_id: AtomicU64,
}

impl RunningJobs {
    fn reserve(&self, job_id: &CrawlJobId) -> Result<RunLease, CrawlError> {
        let key = job_id.to_string();
        let run_id = self.next_run_id.fetch_add(1, Ordering::Relaxed);
        let cancel = CancellationToken::new();

        match self.entries.entry(key.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(RunningJob { run_id, cancel: cancel.clone() });
                Ok(RunLease { job_id: key, run_id, cancel })
            }
            Entry::Occupied(_) => Err(CrawlError::App(AppError::Conflict(format!(
                "crawl job {job_id} is already running"
            )))),
        }
    }

    /// Release only the run that acquired this lease. This prevents a delayed
    /// completion from deleting a newer run registered under the same job ID.
    fn release(&self, lease: &RunLease) {
        self.entries.remove_if(lease.job_id.as_str(), |_, running| {
            running.run_id == lease.run_id
        });
    }

    fn cancel(&self, job_id: &CrawlJobId) -> bool {
        let token = self.entries.get(job_id.as_str()).map(|running| running.cancel.clone());
        match token {
            Some(token) => {
                token.cancel();
                true
            }
            None => false,
        }
    }

    fn remove_and_cancel(&self, job_id: &CrawlJobId) {
        if let Some((_, running)) = self.entries.remove(job_id.as_str()) {
            running.cancel.cancel();
        }
    }
}

pub struct CrawlService {
    pool: SqlitePool,
    knowledge: Arc<KnowledgeService>,
    events: Arc<dyn CrawlEventSink>,
    running: Arc<RunningJobs>,
}

impl CrawlService {
    pub fn new(
        pool: SqlitePool,
        knowledge: Arc<KnowledgeService>,
        events: Arc<dyn CrawlEventSink>,
    ) -> Self {
        Self { pool, knowledge, events, running: Arc::new(RunningJobs::default()) }
    }

    pub async fn create(&self, user_id: &UserId, req: NewJob) -> Result<JobView, CrawlError> {
        if req.seeds.is_empty() {
            return Err(CrawlError::UrlRejected("a crawl job needs at least one seed".into()));
        }
        // Compile the scope now so a bad regex fails at creation, not on the
        // first page fetched an hour later.
        let normalized: Vec<String> = req
            .seeds
            .iter()
            .map(|s| frontier::normalize(s).map(|u| u.to_string()))
            .collect::<Result<_, _>>()?;
        ScopeMatcher::build(&req.scope, &normalized)?;

        let now = nomifun_common::now_ms();
        let job = CrawlJob {
            job_id: CrawlJobId::new(),
            user_id: user_id.clone(),
            name: req.name,
            seeds: normalized,
            scope: req.scope,
            max_depth: req.max_depth,
            max_urls: req.max_urls.max(1),
            render_mode: req.render_mode,
            concurrency: req.concurrency.clamp(1, 64),
            per_host_concurrency: req.per_host_concurrency.clamp(1, 16),
            delay_ms: req.delay_ms,
            respect_robots: req.respect_robots,
            user_agent: None,
            sink: req.sink,
            status: JobStatus::Draft,
            error_detail: None,
            started_at: None,
            finished_at: None,
            created_at: now,
            updated_at: now,
        };
        store::create_job(&self.pool, &job).await?;
        Ok(JobView::new(job, JobProgress::default()))
    }

    pub async fn list(&self, user_id: &UserId) -> Result<Vec<JobView>, CrawlError> {
        let jobs = store::list_jobs(&self.pool, user_id).await?;
        let mut out = Vec::with_capacity(jobs.len());
        for job in jobs {
            let progress = claim::progress(&self.pool, &job.job_id).await?;
            out.push(JobView::new(job, progress));
        }
        Ok(out)
    }

    pub async fn get(&self, user_id: &UserId, job_id: &CrawlJobId) -> Result<JobView, CrawlError> {
        let job = self.owned_job(user_id, job_id).await?;
        let progress = claim::progress(&self.pool, job_id).await?;
        Ok(JobView::new(job, progress))
    }

    pub async fn tasks(
        &self,
        user_id: &UserId,
        job_id: &CrawlJobId,
        status: Option<TaskStatus>,
        limit: u32,
    ) -> Result<Vec<TaskView>, CrawlError> {
        self.owned_job(user_id, job_id).await?;
        let tasks = claim::list_tasks(&self.pool, job_id, status, limit).await?;
        Ok(tasks.into_iter().map(TaskView::from).collect())
    }

    /// Seed the frontier (idempotent) and start the worker pool.
    pub async fn start(&self, user_id: &UserId, job_id: &CrawlJobId) -> Result<JobView, CrawlError> {
        let job = self.owned_job(user_id, job_id).await?;
        let lease = self.running.reserve(job_id)?;
        let mut marked_running = false;

        let prepared = async {
            for seed in &job.seeds {
                let url = frontier::normalize(seed)?;
                let Some(host) = url.host_str() else { continue };
                let discovered = DiscoveredUrl {
                    fingerprint: frontier::fingerprint(&url),
                    host: host.to_ascii_lowercase(),
                    url: url.to_string(),
                    depth: 0,
                    redirect_hops: 0,
                };
                // Seeds outrank discovered links so a restart re-crawls them first.
                claim::enqueue(&self.pool, job_id, None, &discovered, 100).await?;
            }

            let user_agent =
                job.user_agent.clone().unwrap_or_else(|| CRAWLER_USER_AGENT.to_string());
            let matcher = Arc::new(ScopeMatcher::build(&job.scope, &job.seeds)?);
            let politeness = Arc::new(Politeness::new(
                Arc::new(HttpRobotsSource::new(HttpFetcher::new().user_agent(&user_agent))),
                user_agent.clone(),
                job.respect_robots,
                Duration::from_millis(job.delay_ms),
            ));
            let executor = Arc::new(LocalExecutor::new(
                Arc::new(HttpCrawlFetcher::new(&user_agent)),
                politeness,
                Arc::new(KnowledgeSink::new(self.knowledge.clone())),
                matcher,
            ));

            store::start_job(&self.pool, job_id).await?;
            marked_running = true;
            let job = store::get_job(&self.pool, job_id).await?;
            let progress = claim::progress(&self.pool, job_id).await?;
            Ok::<_, CrawlError>((job, executor, progress))
        }
        .await;

        let (job, executor, progress) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => {
                self.running.release(&lease);
                if marked_running {
                    let detail = err.to_string();
                    if let Err(status_err) =
                        store::finish_job(&self.pool, job_id, JobStatus::Failed, Some(&detail)).await
                    {
                        tracing::warn!(
                            job_id = %job_id,
                            error = %status_err,
                            "failed to persist crawl startup failure"
                        );
                    }
                }
                return Err(err);
            }
        };

        let handle = runner::spawn_job(
            self.pool.clone(),
            job.clone(),
            executor,
            self.events.clone(),
            RunnerConfig::default(),
            lease.cancel.clone(),
        );
        let running = self.running.clone();
        tokio::spawn(async move {
            if let Err(err) = handle.wait().await {
                tracing::warn!(job_id = %lease.job_id, error = %err, "crawl runner task failed");
            }
            running.release(&lease);
        });

        Ok(JobView::new(job, progress))
    }

    /// Stop the pool. In-flight tasks are discarded, not settled, so their
    /// leases lapse and they return to `pending`.
    pub async fn cancel(&self, user_id: &UserId, job_id: &CrawlJobId) -> Result<(), CrawlError> {
        self.owned_job(user_id, job_id).await?;
        match self.running.cancel(job_id) {
            true => Ok(()),
            // Not running: park the row so the UI stops showing it as active.
            false => store::set_status(&self.pool, job_id, JobStatus::Cancelled).await,
        }
    }

    pub async fn delete(&self, user_id: &UserId, job_id: &CrawlJobId) -> Result<(), CrawlError> {
        self.owned_job(user_id, job_id).await?;
        self.running.remove_and_cancel(job_id);
        store::delete_job(&self.pool, job_id).await
    }

    /// Requeue every `failed` task so a fixed scope or restored credential can
    /// be retried without recreating the job.
    pub async fn retry_failed(
        &self,
        user_id: &UserId,
        job_id: &CrawlJobId,
    ) -> Result<u64, CrawlError> {
        self.owned_job(user_id, job_id).await?;
        claim::requeue_failed(&self.pool, job_id).await
    }

    async fn owned_job(
        &self,
        user_id: &UserId,
        job_id: &CrawlJobId,
    ) -> Result<CrawlJob, CrawlError> {
        let job = store::get_job(&self.pool, job_id).await?;
        if &job.user_id != user_id {
            // Same answer as a missing job: never confirm another user's ID.
            return Err(CrawlError::JobNotFound(job_id.to_string()));
        }
        Ok(job)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn natural_completion_release_makes_job_restartable() {
        let running = RunningJobs::default();
        let job_id = CrawlJobId::new();

        let first = running.reserve(&job_id).expect("first run reserves the job");
        running.release(&first);

        let second = running.reserve(&job_id).expect("finished job can start again");
        running.release(&second);
    }

    #[tokio::test]
    async fn concurrent_reservations_allow_exactly_one_start() {
        const CALLERS: usize = 16;

        let running = Arc::new(RunningJobs::default());
        let job_id = CrawlJobId::new();
        let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS));
        let mut tasks = Vec::with_capacity(CALLERS);

        for _ in 0..CALLERS {
            let running = running.clone();
            let job_id = job_id.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                running.reserve(&job_id)
            }));
        }

        let mut winner = None;
        let mut conflicts = 0;
        for task in tasks {
            match task.await.expect("reservation task joins") {
                Ok(lease) => {
                    assert!(winner.replace(lease).is_none(), "only one caller may reserve the job");
                }
                Err(CrawlError::App(AppError::Conflict(_))) => conflicts += 1,
                Err(err) => panic!("unexpected reservation error: {err}"),
            }
        }

        assert_eq!(conflicts, CALLERS - 1);
        running.release(&winner.expect("one caller wins the reservation"));
    }

    #[test]
    fn cancellation_keeps_job_reserved_until_runner_stops() {
        let running = RunningJobs::default();
        let job_id = CrawlJobId::new();
        let lease = running.reserve(&job_id).expect("run reserves the job");

        assert!(running.cancel(&job_id));
        assert!(lease.cancel.is_cancelled());
        assert!(matches!(
            running.reserve(&job_id),
            Err(CrawlError::App(AppError::Conflict(_)))
        ));

        running.release(&lease);
        let restarted = running.reserve(&job_id).expect("job restarts after runner exits");
        running.release(&restarted);
    }

    #[test]
    fn delayed_cleanup_cannot_remove_a_newer_run() {
        let running = RunningJobs::default();
        let job_id = CrawlJobId::new();
        let old = running.reserve(&job_id).expect("old run reserves the job");
        running.remove_and_cancel(&job_id);
        let new = running.reserve(&job_id).expect("new run reserves the removed slot");

        running.release(&old);
        assert!(matches!(
            running.reserve(&job_id),
            Err(CrawlError::App(AppError::Conflict(_)))
        ));

        running.release(&new);
    }
}
