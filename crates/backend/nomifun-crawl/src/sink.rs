//! Where crawled pages land.
//!
//! Default target is the knowledge base, through its managed-document replace
//! path. Crawled pages are complete snapshots, not append-only chat updates.

use std::sync::Arc;

use nomifun_common::{CrawlJobId, KnowledgeBaseId};
use nomifun_knowledge::service::{KnowledgeService, ManagedDocumentWriteRequest};
use nomifun_knowledge::source_url::slug_for_url;
use url::Url;

use crate::error::CrawlError;
use crate::model::CrawlJob;

/// Root directory (inside the base) that the crawler owns. The URL-source
/// snapshots own `snapshots/` and are rebuilt wholesale from their entries, so
/// the two must never share a directory.
pub const CRAWL_REL_DIR: &str = "crawl";

/// How much of the job id is folded into the directory name.
const JOB_ID_SUFFIX_LEN: usize = 8;

/// The *tail* of the job id. A UUIDv7's leading hex digits are the millisecond
/// timestamp, whose top 32 bits only change every ~65s — a head slice would
/// hand two jobs created in the same minute the same directory, which is the
/// collision this suffix exists to prevent. The tail is the random half.
fn id_suffix(job_id: &CrawlJobId) -> &str {
    let raw = job_id.as_str();
    raw.get(raw.len().saturating_sub(JOB_ID_SUFFIX_LEN)..).unwrap_or(raw)
}

#[derive(Debug, Clone)]
pub struct IngestPage {
    pub url: String,
    pub url_fingerprint: String,
    pub claim_generation: i64,
    pub content_hash: String,
    pub title: Option<String>,
    pub markdown: String,
}

/// Result of persisting one page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestReceipt {
    pub rel_path: String,
}

#[async_trait::async_trait]
pub trait CrawlSinkWriter: Send + Sync {
    /// Persist one page. `Ok(None)` means the job has no configured target and
    /// the content was intentionally dropped.
    async fn write(&self, job: &CrawlJob, page: &IngestPage)
    -> Result<Option<IngestReceipt>, CrawlError>;
}

pub struct KnowledgeSink {
    service: Arc<KnowledgeService>,
}

impl KnowledgeSink {
    pub fn new(service: Arc<KnowledgeService>) -> Self {
        Self { service }
    }
}

#[async_trait::async_trait]
impl CrawlSinkWriter for KnowledgeSink {
    async fn write(
        &self,
        job: &CrawlJob,
        page: &IngestPage,
    ) -> Result<Option<IngestReceipt>, CrawlError> {
        let Some(raw_kb_id) = job.sink.knowledge_base_id.clone() else {
            return Ok(None);
        };
        let kb_id = KnowledgeBaseId::parse(raw_kb_id)
            .map_err(|e| CrawlError::UrlRejected(format!("invalid knowledge base id: {e}")))?;
        let rel_path = document_path(
            &job.job_id,
            &job.name,
            &page.url,
            &page.url_fingerprint,
        );
        let document_key = format!("{}:{}", job.job_id, page.url_fingerprint);
        let request = ManagedDocumentWriteRequest {
            kb_id,
            rel_path,
            namespace: CRAWL_REL_DIR.into(),
            producer: "nomifun-crawl".into(),
            document_key,
            generation: page.claim_generation,
            content: render_document(&job.job_id, page),
        };
        let outcome = self.service.replace_managed_document(request).await?;
        Ok(Some(IngestReceipt { rel_path: outcome.final_rel_path }))
    }
}

/// `crawl/{job}-{id8}/{readable-url}-{full-fingerprint}.md`.
///
/// The readable prefix is only decoration. The full normalized-URL
/// fingerprint is the identity, so query strings, ports and long common path
/// prefixes cannot collapse onto one file.
pub fn document_path(
    job_id: &CrawlJobId,
    job_name: &str,
    url: &str,
    url_fingerprint: &str,
) -> String {
    let readable = Url::parse(url)
        .map(|u| slug_for_url(&u))
        .unwrap_or_else(|_| "page".to_string());
    let readable = readable.chars().take(48).collect::<String>();
    let readable = readable.trim_matches('-');
    let readable = if readable.is_empty() { "page" } else { readable };
    format!(
        "{CRAWL_REL_DIR}/{}-{}/{readable}-{url_fingerprint}.md",
        slugify(job_name),
        id_suffix(job_id)
    )
}

/// Front matter carries provenance so readers can tell where the text came
/// from without opening the crawl UI.
fn render_document(job_id: &CrawlJobId, page: &IngestPage) -> String {
    let title = page.title.clone().unwrap_or_else(|| page.url.clone());
    format!(
        concat!(
            "---\n",
            "title: {}\n",
            "source_url: {}\n",
            "source: nomifun-crawl\n",
            "managed_by: nomifun-crawl\n",
            "managed_key: {}:{}\n",
            "managed_generation: {}\n",
            "crawl_job_id: {}\n",
            "url_fingerprint: {}\n",
            "content_hash: {}\n",
            "---\n\n{}\n"
        ),
        yaml_scalar(&title),
        yaml_scalar(&page.url),
        job_id,
        page.url_fingerprint,
        page.claim_generation,
        job_id,
        page.url_fingerprint,
        page.content_hash,
        page.markdown.trim(),
    )
}

/// Quote anything that YAML would otherwise reinterpret.
fn yaml_scalar(value: &str) -> String {
    let cleaned = value.replace(['\n', '\r'], " ");
    format!("\"{}\"", cleaned.replace('\\', "\\\\").replace('"', "\\\""))
}

fn slugify(value: &str) -> String {
    let mut out = String::new();
    let mut last_dash = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash && !out.is_empty() {
            out.push('-');
            last_dash = true;
        }
        if out.len() >= 60 {
            break;
        }
    }
    let trimmed = out.trim_matches('-').to_string();
    if trimmed.is_empty() { "job".to_string() } else { trimmed }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontier::{fingerprint, normalize};

    fn fingerprint_for(url: &str) -> String {
        fingerprint(&normalize(url).unwrap())
    }

    fn page(url: &str) -> IngestPage {
        IngestPage {
            url: url.into(),
            url_fingerprint: fingerprint_for(url),
            claim_generation: 3,
            content_hash: "c".repeat(64),
            title: None,
            markdown: "body".into(),
        }
    }

    #[test]
    fn path_groups_by_job_and_slugs_the_url() {
        let job_id = CrawlJobId::new();
        let url = "https://example.com/docs/intro";
        let path = document_path(&job_id, "My Site Crawl", url, &fingerprint_for(url));
        assert!(path.starts_with(&format!("crawl/my-site-crawl-{}/", id_suffix(&job_id))), "{path}");
        assert!(path.ends_with(".md"), "{path}");
        assert!(path.contains(&fingerprint_for(url)), "{path}");
    }

    #[test]
    fn url_variants_that_used_to_share_a_slug_get_distinct_paths() {
        let job_id = CrawlJobId::new();
        for (a, b) in [
            ("https://example.com/page?a=1", "https://example.com/page?a=2"),
            ("https://example.com:8443/page", "https://example.com:9443/page"),
            ("http://example.com/page", "https://example.com/page"),
            ("https://example.com/a+b", "https://example.com/a-b"),
        ] {
            assert_ne!(
                document_path(&job_id, "j", a, &fingerprint_for(a)),
                document_path(&job_id, "j", b, &fingerprint_for(b)),
                "{a} and {b} must not collide"
            );
        }
    }

    /// The whole point of grouping by job: without the id suffix two jobs
    /// sharing a name (or a name that only differs past the slug cap) write
    /// the same file and silently overwrite each other. Ids minted back to
    /// back are the hard case — a UUIDv7 prefix would still be identical.
    #[test]
    fn same_named_jobs_do_not_share_a_directory() {
        let url = "https://example.com/a";
        let url_fingerprint = fingerprint_for(url);
        let a = document_path(&CrawlJobId::new(), "Docs", url, &url_fingerprint);
        let b = document_path(&CrawlJobId::new(), "Docs", url, &url_fingerprint);
        assert_ne!(a, b);
    }

    #[test]
    fn unparseable_url_still_yields_a_path() {
        let job_id = CrawlJobId::new();
        let url_fingerprint = "a".repeat(64);
        assert_eq!(
            document_path(&job_id, "j", "not a url", &url_fingerprint),
            format!("crawl/j-{}/page-{url_fingerprint}.md", id_suffix(&job_id))
        );
    }

    #[test]
    fn job_name_of_only_symbols_falls_back() {
        let job_id = CrawlJobId::new();
        let url = "https://e.com/x";
        let path = document_path(&job_id, "!!!", url, &fingerprint_for(url));
        assert_eq!(path.split('/').nth(1), Some(format!("job-{}", id_suffix(&job_id)).as_str()));
    }

    #[test]
    fn front_matter_escapes_quotes_and_newlines() {
        let job_id = CrawlJobId::new();
        let mut page = page("https://e.com/x");
        page.title = Some("A \"quoted\"\ntitle".into());
        let doc = render_document(&job_id, &page);
        assert!(doc.contains(r#"title: "A \"quoted\" title""#), "{doc}");
        assert!(doc.contains("source_url: \"https://e.com/x\""));
        assert!(doc.contains("managed_by: nomifun-crawl"), "{doc}");
        assert!(doc.contains(&format!("managed_key: {job_id}:{}", page.url_fingerprint)), "{doc}");
        assert!(doc.contains("managed_generation: 3"), "{doc}");
    }

    #[test]
    fn missing_title_falls_back_to_the_url() {
        let doc = render_document(&CrawlJobId::new(), &page("https://e.com/x"));
        assert!(doc.contains(r#"title: "https://e.com/x""#), "{doc}");
    }
}
