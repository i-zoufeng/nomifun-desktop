-- Redirects are frontier tasks, not hidden HTTP-client hops. Persist the hop
-- count so a long chain cannot evade the crawler's redirect bound by crossing
-- worker claims.
ALTER TABLE crawl_tasks
ADD COLUMN redirect_hops INTEGER NOT NULL DEFAULT 0 CHECK (redirect_hops >= 0);
