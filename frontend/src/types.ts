export type ServiceState = {
  healthy: boolean;
  detail: string;
  pid?: number | null;
};

export type QueueState = {
  depth: number;
  oldest_age_seconds: number;
  expiring: number;
  expiry_urgent: number;
};

export type DailyCounters = {
  events: number;
  images_seen: number;
  image_segments: number;
  cdn_requests: number;
  cdn_downloads: number;
  cdn_bytes: number;
  cdn_400: number;
  cdn_403: number;
  cdn_429: number;
  history_calls: number;
  window_history_calls: number;
  get_image_blocked: number;
  accepted: number;
  rejected: number;
  duplicates: number;
  failed: number;
  expired: number;
  filtered_gif: number;
};

export type GroupRuntime = {
  group_id: string;
  display_name: string | null;
  enabled: number;
  event_status: string | null;
  last_message_id: string | null;
  last_message_time: number | null;
  last_event_at: number | null;
  last_image_at: number | null;
  gap_status: string | null;
  gap_started_at: number | null;
  gap_finished_at: number | null;
  gap_error: string | null;
  accepted: number;
  duplicates: number;
  rejected: number;
  failed: number;
  expired: number;
  queued: number;
};

export type Job = {
  id: number;
  kind: "gap_recovery";
  group_id: string;
  status: string;
  progress_pages: number;
  cancel_requested: number;
  created_at: number;
  started_at: number | null;
  updated_at: number;
  finished_at: number | null;
  error: string | null;
};

export type DashboardStatus = {
  timestamp: number;
  /** Age of the served snapshot. Large values mean the console is frozen. */
  snapshot_age_seconds?: number;
  /** True until the first real compute lands; the view is starting, not stale. */
  snapshot_starting?: boolean;
  services: Record<string, ServiceState>;
  account: { user_id: string; nickname: string } | null;
  action: {
    name: string | null;
    status: string;
    stage: string | null;
    message: string | null;
    error: string | null;
  };
  migration: {
    status: string;
    stage: string | null;
    current: number;
    total: number;
    error: string | null;
  };
  statistics: {
    unique_images: number;
    accepted_records: number;
    novelai: number;
    comfyui: number;
    novelai_unreadable: number;
    other_models: number;
    disk_bytes: number;
    queue: QueueState;
    today: DailyCounters;
    events: Record<string, unknown>;
    downloader: Record<string, unknown>;
    window_recovery: Record<string, unknown>;
  };
  groups: GroupRuntime[];
  jobs: Job[];
  setup: {
    completed: boolean;
    checks: { key: string; label: string; ok: boolean; detail: string }[];
  };
  access: {
    mode: "local" | "remote" | "direct";
    identity: { email: string } | null;
    permissions: string[];
  };
};

export type Settings = {
  storage_root?: string;
  deployment_mode?: "linux-docker";
  external_services?: boolean;
  download_interval_seconds: number;
  download_jitter_seconds: number;
  url_preference: "data" | "raw";
  collector_paused: boolean;
  unlimited_collection: boolean;
  remote_restricted?: boolean;
};

export type AvailableGroup = {
  group_id: string;
  group_name: string;
  member_count: number;
  max_member_count: number;
};
