export type ServiceState = {
  healthy: boolean;
  detail: string;
  pid?: number | null;
};

export type GroupRuntime = {
  group_id: string;
  display_name: string | null;
  enabled: number;
  recent_status: string | null;
  recent_last_success: number | null;
  recent_last_error: string | null;
  recent_cursor_time: number | null;
  backfill_status: string | null;
  backfill_cursor_time: number | null;
  backfill_completed: number;
  backfill_last_success: number | null;
  backfill_last_error: string | null;
  accepted: number;
  duplicates: number;
  rejected: number;
  failed: number;
};

export type Job = {
  id: number;
  kind: "page" | "continuous" | "rescan";
  group_id: string;
  status: string;
  progress_pages: number;
  cancel_requested: number;
  created_at: number;
  updated_at: number;
  error: string | null;
};

export type DashboardStatus = {
  timestamp: number;
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
    today_new: number;
    disk_bytes: number;
    gif_excluded: number;
    failed: number;
    rejected: number;
    resolving: number;
    assets: number;
    provenance_missing: number;
    provenance_complete: number;
  };
  groups: GroupRuntime[];
  jobs: Job[];
  setup: {
    completed: boolean;
    ready: boolean;
    checks: { key: string; label: string; ok: boolean; detail: string }[];
    links: Record<string, string>;
  };
  access: {
    mode: "local" | "remote";
    identity: { email: string } | null;
    permissions: string[];
  };
  remote: {
    enabled: boolean;
    public_origin: string | null;
    snapshot: {
      enabled: boolean;
      last_success?: number | null;
      last_error?: string | null;
    };
  };
};

export type Settings = {
  storage_root?: string;
  deployment_mode?: "linux-docker";
  external_services?: boolean;
  qq_path?: string;
  napcat_root?: string;
  launcher_kind?: "framework" | "shell" | "external";
  shell_launcher?: string | null;
  poll_interval_seconds: number;
  catchup_page_size: number;
  backfill_page_size: number;
  collector_paused: boolean;
  backfill_paused: boolean;
  deep_backfill_enabled?: boolean;
  remote_restricted?: boolean;
};

export type AvailableGroup = {
  group_id: string;
  group_name: string;
  member_count: number;
  max_member_count: number;
};
