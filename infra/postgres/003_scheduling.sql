-- Phase 3: Add priority and delayed_until columns for scheduling.
ALTER TABLE jobs ADD COLUMN priority TEXT DEFAULT 'normal';
ALTER TABLE jobs ADD COLUMN delayed_until TIMESTAMPTZ;
