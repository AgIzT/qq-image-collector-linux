import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import type { AvailableGroup, DashboardStatus, GroupRuntime, Job, Settings } from "./types";

type View = "overview" | "groups" | "jobs" | "settings" | "logs";
type SystemAction = "start" | "stop" | "restart";
type SessionInfo = {
  ok: boolean;
  mode: "local" | "remote" | "direct";
  identity: { email: string } | null;
  csrf_token: string | null;
  permissions: string[];
};

let activeCsrfToken: string | null = null;

class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function api<T>(url: string, options: RequestInit = {}): Promise<T> {
  const method = String(options.method ?? "GET").toUpperCase();
  const mutation = ["POST", "PUT", "PATCH", "DELETE"].includes(method);
  const response = await fetch(url, {
    credentials: "same-origin",
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(mutation && activeCsrfToken ? { "X-CSRF-Token": activeCsrfToken } : {}),
      ...(options.headers ?? {}),
    },
  });
  const payload = (await response.json().catch(() => ({}))) as Record<string, unknown>;
  if (!response.ok) throw new ApiError(response.status, String(payload.detail ?? `HTTP ${response.status}`));
  return payload as T;
}

function formatTime(value: number | null | undefined): string {
  if (!value) return "—";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit",
  }).format(new Date(value * 1000));
}

function formatDuration(value: number | null | undefined): string {
  const seconds = Math.max(0, Number(value ?? 0));
  if (seconds < 60) return `${seconds} 秒`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)} 分钟`;
  return `${(seconds / 3600).toFixed(1)} 小时`;
}

function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let size = value;
  let index = -1;
  do { size /= 1024; index += 1; } while (size >= 1024 && index < units.length - 1);
  return `${size.toFixed(size >= 10 ? 1 : 2)} ${units[index]}`;
}

const labels: Record<string, string> = {
  idle: "空闲", running: "运行中", connected: "已连接", disconnected: "已断开",
  queued: "排队中", cancelled: "已取消", completed: "已完成", failed: "失败",
  error: "异常", paused: "已暂停", recovered: "已恢复", incomplete: "未完全恢复",
  no_gap: "无断档", downloading: "下载中", receiving: "接收中",
  recovering: "恢复中", complete: "已恢复", partial: "部分恢复", deferred: "已延后",
};

function StatusPill({ value, good }: { value: string | null | undefined; good?: boolean }) {
  const normalized = value || "idle";
  const tone = good || ["connected", "completed", "complete", "recovered", "receiving", "no_gap"].includes(normalized)
    ? "good"
    : ["failed", "error", "incomplete", "disconnected"].includes(normalized)
      ? "bad"
      : ["running", "queued", "downloading"].includes(normalized) ? "active" : "neutral";
  return <span className={`pill ${tone}`}>{labels[normalized] ?? normalized}</span>;
}

function ServiceCard({ title, state }: { title: string; state?: { healthy: boolean; detail: string } }) {
  const healthy = Boolean(state?.healthy);
  return (
    <article className={`service-card ${healthy ? "online" : "offline"}`}>
      <div className="service-heading"><span className="service-dot" /><span>{title}</span></div>
      <strong>{healthy ? "正常" : "未就绪"}</strong>
      <small title={state?.detail}>{state?.detail ?? "等待状态"}</small>
    </article>
  );
}

function Metric({ label, value, note }: { label: string; value: string | number; note?: string }) {
  return <article className="metric"><span>{label}</span><strong>{value}</strong>{note && <small>{note}</small>}</article>;
}

function GroupCard({ group, onDisable, onEnable }: {
  group: GroupRuntime;
  onDisable: (id: string) => void;
  onEnable: (id: string, name?: string) => void;
}) {
  return (
    <article className={`group-card ${group.enabled ? "" : "disabled"}`}>
      <div className="group-title">
        <div><h3>{group.display_name || `群 ${group.group_id}`}</h3><code>{group.group_id}</code></div>
        <StatusPill value={group.enabled ? "connected" : "paused"} good={Boolean(group.enabled)} />
      </div>
      <div className="runtime-grid">
        <div><span>事件监听</span><StatusPill value={group.event_status} /><small>最后消息 {formatTime(group.last_event_at)}</small></div>
        <div><span>断档恢复</span><StatusPill value={group.gap_status} /><small>最后图片 {formatTime(group.last_image_at)}</small>{group.gap_error && <small className="error-text">{group.gap_error}</small>}</div>
      </div>
      <div className="group-counts">
        <span><b>{group.queued}</b> 排队</span><span><b>{group.accepted}</b> 有效</span>
        <span><b>{group.rejected}</b> 淘汰</span><span><b>{group.duplicates}</b> 重复</span><span><b>{group.expired}</b> URL失效</span><span><b>{group.failed}</b> 失败</span>
      </div>
      <div className="button-row compact">
        {group.enabled ? <>
          <button className="danger-ghost" onClick={() => onDisable(group.group_id)}>停止监听</button>
        </> : <button className="primary" onClick={() => onEnable(group.group_id, group.display_name ?? undefined)}>重新启用</button>}
      </div>
    </article>
  );
}

export default function App() {
  const [view, setView] = useState<View>("overview");
  const [dashboard, setDashboard] = useState<DashboardStatus | null>(null);
  const [session, setSession] = useState<SessionInfo | null>(null);
  const [authorized, setAuthorized] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const [settings, setSettings] = useState<Settings | null>(null);
  const [available, setAvailable] = useState<AvailableGroup[]>([]);
  const [search, setSearch] = useState("");
  const [manualGroup, setManualGroup] = useState("");
  const [logs, setLogs] = useState<{ path: string; lines: string[] }[]>([]);

  const notify = useCallback((message: string) => {
    setToast(message);
    window.setTimeout(() => setToast(null), 3500);
  }, []);
  const refresh = useCallback(async () => setDashboard(await api<DashboardStatus>("/api/v1/status")), []);

  useEffect(() => {
    void (async () => {
      try {
        const params = new URLSearchParams(window.location.search);
        const token = params.get("session_token");
        const established = await api<SessionInfo>(token ? `/api/v1/session?session_token=${encodeURIComponent(token)}` : "/api/v1/session");
        if (token) window.history.replaceState({}, "", `${window.location.pathname}${window.location.hash}`);
        activeCsrfToken = established.csrf_token;
        setSession(established);
        await refresh();
        setAuthorized(true);
      } catch (error) { setAuthError(error instanceof Error ? error.message : "无法建立控制台会话"); }
    })();
  }, [refresh]);

  useEffect(() => {
    if (!authorized) return;
    const source = new EventSource("/api/v1/events", { withCredentials: true });
    source.addEventListener("status", (event) => setDashboard(JSON.parse((event as MessageEvent).data) as DashboardStatus));
    let fallback: number | null = null;
    source.onopen = () => {
      if (fallback !== null) window.clearInterval(fallback);
      fallback = null;
    };
    source.onerror = () => {
      if (fallback === null) {
        fallback = window.setInterval(() => void refresh().catch(() => undefined), 7000);
      }
    };
    return () => {
      source.close();
      if (fallback !== null) window.clearInterval(fallback);
    };
  }, [authorized, refresh]);

  useEffect(() => {
    if (!authorized || view !== "settings") return;
    void api<Settings>("/api/v1/settings").then(setSettings).catch((error: Error) => notify(error.message));
  }, [authorized, view, notify]);

  useEffect(() => {
    if (!authorized || view !== "logs" || session?.mode === "remote") return;
    void api<{ files: { path: string; lines: string[] }[] }>("/api/v1/logs?lines=250").then((value) => setLogs(value.files)).catch((error: Error) => notify(error.message));
  }, [authorized, view, notify, session?.mode]);

  const systemAction = async (action: SystemAction) => {
    try {
      await api(`/api/v1/system/${action}`, { method: "POST", body: JSON.stringify({ confirm_close_qq: true }) });
      notify(action === "stop" ? "正在停止事件 Worker" : "操作已开始");
      await refresh();
    } catch (error) { notify(error instanceof Error ? error.message : "操作失败"); }
  };

  const addGroup = async (groupId: string, displayName?: string) => {
    try {
      await api("/api/v1/groups", { method: "POST", body: JSON.stringify({ group_id: groupId.trim(), display_name: displayName || null }) });
      setManualGroup(""); notify("监听群已启用；首次从当前时刻开始"); await refresh();
    } catch (error) { notify(error instanceof Error ? error.message : "添加失败"); }
  };

  const disableGroup = async (groupId: string) => {
    if (!window.confirm(`停止监听群 ${groupId}？已有图片和游标会保留。`)) return;
    try { await api(`/api/v1/groups/${groupId}`, { method: "DELETE" }); notify("已停止监听"); await refresh(); }
    catch (error) { notify(error instanceof Error ? error.message : "操作失败"); }
  };

  const loadAvailable = async () => {
    try { const rows = await api<AvailableGroup[]>("/api/v1/groups/available"); setAvailable(rows); notify(`已读取 ${rows.length} 个群`); }
    catch (error) { notify(error instanceof Error ? error.message : "读取群列表失败"); }
  };

  const cancelJob = async (job: Job) => {
    try { await api(`/api/v1/jobs/${job.id}/cancel`, { method: "POST", body: "{}" }); notify("将在当前页边界取消"); await refresh(); }
    catch (error) { notify(error instanceof Error ? error.message : "取消失败"); }
  };

  const recoverGap = async (groupId: string) => {
    try {
      await api(`/api/v1/groups/${groupId}/recover-gap`, { method: "POST", body: "{}" });
      notify(`群 ${groupId} 的断档恢复已排队`);
      await refresh();
    } catch (error) { notify(error instanceof Error ? error.message : "恢复任务创建失败"); }
  };

  const saveSettings = async (event: FormEvent) => {
    event.preventDefault(); if (!settings) return;
    try {
      const payload = {
        download_interval_seconds: settings.download_interval_seconds,
        download_jitter_seconds: settings.download_jitter_seconds,
        url_preference: settings.url_preference,
        collector_paused: settings.collector_paused,
      };
      setSettings(await api<Settings>("/api/v1/settings", { method: "PATCH", body: JSON.stringify(payload) }));
      notify("设置已保存，Worker 下一循环生效");
    } catch (error) { notify(error instanceof Error ? error.message : "保存失败"); }
  };

  const filtered = useMemo(() => {
    const query = search.trim().toLowerCase();
    return query ? available.filter((row) => `${row.group_id} ${row.group_name}`.toLowerCase().includes(query)) : available;
  }, [available, search]);

  if (authError) return <main className="center-screen"><div className="auth-card"><div className="brand-mark">AI</div><h1>无法进入控制台</h1><p>{authError}</p></div></main>;
  if (!dashboard) return <main className="center-screen"><div className="loader" /><p>正在连接事件采集服务…</p></main>;

  const workerRunning = Boolean(dashboard.services.worker?.healthy);
  const remoteMode = session?.mode === "remote";
  const accountName = dashboard.account ? `${dashboard.account.nickname || "QQ"} · ${dashboard.account.user_id}` : "等待 QQ 扫码登录";
  const today = dashboard.statistics.today;
  const queue = dashboard.statistics.queue;
  const downloader = dashboard.statistics.downloader;
  const windowRecovery = dashboard.statistics.window_recovery ?? {};
  const eventState = dashboard.statistics.events;

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand"><div className="brand-mark">AI</div><div><strong>原图采集</strong><small>EVENT PIPELINE</small></div></div>
        <nav>{([
          ["overview", "运行总览", "⌂"], ["groups", "监听群聊", "◎"], ["jobs", "断档任务", "↺"], ["settings", "采集设置", "⚙"],
          ...(!remoteMode ? [["logs", "运行日志", "≡"]] : []),
        ] as [View, string, string][]).map(([key, label, icon]) => <button key={key} className={view === key ? "active" : ""} onClick={() => setView(key)}><span>{icon}</span>{label}</button>)}</nav>
        <div className="sidebar-footer"><span className={`live-dot ${workerRunning ? "on" : ""}`} /><div><strong>{workerRunning ? "事件采集中" : "Worker 已停止"}</strong><small>{accountName}</small></div></div>
      </aside>

      <main className="content">
        <header className="topbar"><div><p className="eyebrow">QQ AI IMAGE COLLECTOR</p><h1>{{ overview: "运行总览", groups: "监听群聊", jobs: "断档恢复任务", settings: "采集设置", logs: "运行日志" }[view]}</h1></div><div className="top-actions"><button className="secondary" onClick={() => void systemAction("restart")}>重启 Worker</button>{workerRunning ? <button className="danger" onClick={() => void systemAction("stop")}>停止 Worker</button> : <button className="primary" onClick={() => void systemAction("start")}>启动 Worker</button>}</div></header>

        {remoteMode && <section className="remote-banner"><div><strong>Cloudflare Access 远程会话</strong><small>{session?.identity?.email}</small></div><span>控制权限受 Token、CSRF 与身份验证保护</span></section>}
        {dashboard.action.status !== "idle" && <section className={`action-banner ${dashboard.action.status}`}><div className={dashboard.action.status === "running" ? "spinner" : "action-icon"}>{dashboard.action.status === "completed" ? "✓" : dashboard.action.status === "failed" ? "!" : ""}</div><div><strong>{dashboard.action.message || "系统操作"}</strong><small>{dashboard.action.error || dashboard.action.stage || ""}</small></div></section>}

        {view === "overview" && <>
          <section className="account-strip"><div><span className="avatar">Q</span><div><small>当前登录 QQ</small><strong>{accountName}</strong></div></div><span>刷新于 {formatTime(dashboard.timestamp)}</span></section>
          <section className="service-grid">
            <ServiceCard title="NapCat" state={dashboard.services.napcat} /><ServiceCard title="OneBot HTTP" state={dashboard.services.onebot} />
            <ServiceCard title="OneBot WS" state={dashboard.services.event_stream} /><ServiceCard title="持久队列" state={dashboard.services.queue} />
            <ServiceCard title="CDN 下载器" state={dashboard.services.downloader} /><ServiceCard title="自动断档恢复" state={dashboard.services.recovery} />
          </section>
          <section className="section-heading"><div><p className="eyebrow">TODAY</p><h2>事件与 CDN 链路</h2></div></section>
          <section className="metric-grid">
            <Metric label="今日事件" value={today.events} note={`${today.image_segments} 个图片段`} />
            <Metric label="队列深度" value={queue.depth} note={`最老 ${formatDuration(queue.oldest_age_seconds)} · ${queue.expiry_urgent} 条临期`} />
            <Metric label="CDN 请求 / 完整下载" value={`${today.cdn_requests} / ${today.cdn_downloads}`} note={formatBytes(today.cdn_bytes)} />
            <Metric label="有效新增" value={today.accepted} note={`${today.duplicates} 个重复`} />
            <Metric label="CDN 400 / 403" value={`${today.cdn_400} / ${today.cdn_403}`} note="URL 失效候选 / 拒绝" />
            <Metric label="CDN 429" value={today.cdn_429} note="仅延后当前图片，其他任务继续" />
            <Metric label="URL 已失效" value={today.expired} note="独立告警，不并入普通失败" />
            <Metric label="历史调用" value={today.history_calls} note={`仅断档/URL恢复 · 窗口补漏 ${today.window_history_calls ?? 0}`} />
            <Metric
              label="限定窗口补漏"
              value={`${Number(windowRecovery.groups_terminal ?? 0)} / ${Number(windowRecovery.groups_total ?? 0)}`}
              note={`${String(windowRecovery.phase ?? "未启动")} · 新入队 ${Number(windowRecovery.images_enqueued ?? 0)}`}
            />
            <Metric label="拦截 get_image" value={today.get_image_blocked} note="必须始终为 0" />
            <Metric label="下载器状态" value={String(downloader.status ?? "idle")} />
          </section>
          <section className="section-heading"><div><p className="eyebrow">ARCHIVE</p><h2>四分类仓库</h2></div></section>
          <section className="metric-grid">
            <Metric label="去重有效图片" value={dashboard.statistics.unique_images} note={`${dashboard.statistics.accepted_records} 条消息记录`} />
            <Metric label="NovelAI" value={dashboard.statistics.novelai} /><Metric label="ComfyUI" value={dashboard.statistics.comfyui} />
            <Metric label="NAI含参但不可直接读取的" value={dashboard.statistics.novelai_unreadable} /><Metric label="其他模型生成" value={dashboard.statistics.other_models} />
            <Metric label="图库占用" value={formatBytes(dashboard.statistics.disk_bytes)} />
          </section>
          <section className="compact-table">{dashboard.groups.filter((row) => row.enabled).map((group) => <div className="compact-row" key={group.group_id}><div><strong>{group.display_name || group.group_id}</strong><small>{group.group_id}</small></div><div><small>最后事件</small><strong>{formatTime(group.last_event_at)}</strong></div><div><small>最后图片</small><strong>{formatTime(group.last_image_at)}</strong></div><div><small>排队 / 有效</small><strong>{group.queued} / {group.accepted}</strong></div></div>)}</section>
        </>}

        {view === "groups" && <>
          <section className="panel add-panel"><div><p className="eyebrow">MONITOR TARGETS</p><h2>监听对象</h2><p>新群首次从当前时刻开始，不自动追溯历史。</p></div><form onSubmit={(event) => { event.preventDefault(); void addGroup(manualGroup); }}><input value={manualGroup} onChange={(event) => setManualGroup(event.target.value.replace(/\D/g, ""))} placeholder="输入 QQ 群号" /><button className="primary" disabled={!manualGroup}>加入监听</button></form><button className="secondary" onClick={() => void loadAvailable()}>读取当前 QQ 群列表</button></section>
          {available.length > 0 && <section className="panel available-panel"><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="搜索群名或群号" /><div className="available-list">{filtered.slice(0, 100).map((row) => <div key={row.group_id}><div><strong>{row.group_name || "未命名群"}</strong><small>{row.group_id} · {row.member_count} 人</small></div><button className="secondary" onClick={() => void addGroup(row.group_id, row.group_name)}>监听</button></div>)}</div></section>}
          <section className="group-list">{dashboard.groups.map((group) => <GroupCard key={group.group_id} group={group} onDisable={(id) => void disableGroup(id)} onEnable={(id, name) => void addGroup(id, name)} />)}</section>
        </>}

        {view === "jobs" && <>
          <section className="panel"><div className="section-heading"><div><p className="eyebrow">GAP RECOVERY</p><h2>按持久游标补断档</h2><p>断线和重启会自动补齐；这里可手动重跑当前游标之后的缺口，不会向更早历史倒扫。</p></div></div><div className="button-row compact">{dashboard.groups.filter((group) => group.enabled).map((group) => <button className="secondary" key={group.group_id} onClick={() => void recoverGap(group.group_id)}>恢复 {group.display_name || group.group_id}</button>)}</div></section>
          <section className="panel"><div className="job-table"><div className="job-row header"><span>任务</span><span>群号</span><span>状态</span><span>页数</span><span>更新时间</span><span /></div>{dashboard.jobs.map((job) => <div className="job-row" key={job.id}><span>#{job.id} · 断档恢复</span><code>{job.group_id}</code><StatusPill value={job.status} /><span>{job.progress_pages}</span><span>{formatTime(job.updated_at)}</span><span>{["queued", "running"].includes(job.status) && <button className="danger-ghost" onClick={() => void cancelJob(job)}>安全取消</button>}</span>{job.error && <small className="job-error">{job.error}</small>}</div>)}{!dashboard.jobs.length && <div className="empty">当前没有断档任务</div>}</div></section>
        </>}

        {view === "settings" && settings && <>
          <form className="panel settings-form" onSubmit={(event) => void saveSettings(event)}><div className="section-heading"><div><p className="eyebrow">COLLECTION</p><h2>下载节奏</h2></div><button className="primary" type="submit">保存设置</button></div><div className="form-grid">
            <label><span>图片间隔（秒）</span><input type="number" min={5} max={3600} value={settings.download_interval_seconds} onChange={(event) => setSettings({ ...settings, download_interval_seconds: Number(event.target.value) })} /></label>
            <label><span>随机抖动（秒）</span><input type="number" min={0} max={60} value={settings.download_jitter_seconds} onChange={(event) => setSettings({ ...settings, download_jitter_seconds: Number(event.target.value) })} /></label>
            <label><span>CDN 首选通道</span><select value={settings.url_preference} onChange={(event) => setSettings({ ...settings, url_preference: event.target.value as "data" | "raw" })}><option value="data">标准 data.url</option><option value="raw">raw originImageUrl</option></select></label>
          </div><div className="toggle-grid"><label><input type="checkbox" checked={settings.collector_paused} onChange={(event) => setSettings({ ...settings, collector_paused: event.target.checked })} /><span><strong>暂停下载</strong><small>事件仍持久化，队列保留</small></span></label></div></form>
          <section className="panel storage-panel"><div><p className="eyebrow">UNLIMITED</p><h2>无限采集与自动恢复</h2><p>当前仓库：<code>{settings.storage_root}</code></p></div><small className="muted">没有每日数量、历史次数或 403/429 全局熔断。每个图片事件都会持久化并处理；网络失败持续延期重试，WS 断线后按群游标自动补齐。生产 Worker 仍不调用 get_image。</small></section>
        </>}

        {view === "logs" && <section className="panel logs-panel"><div className="section-heading"><div><p className="eyebrow">DIAGNOSTICS</p><h2>最近运行日志</h2></div><button className="secondary" onClick={() => void api<{ files: { path: string; lines: string[] }[] }>("/api/v1/logs?lines=250").then((value) => setLogs(value.files))}>刷新</button></div>{logs.map((file) => <div className="log-file" key={file.path}><strong>{file.path}</strong><pre>{file.lines.join("\n") || "（空）"}</pre></div>)}{!logs.length && <div className="empty">尚无日志</div>}</section>}
      </main>
      {toast && <div className="toast">{toast}</div>}
      <span hidden>{String(eventState.connected ?? false)}</span>
    </div>
  );
}
