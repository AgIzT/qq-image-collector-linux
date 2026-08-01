import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import type { AvailableGroup, DashboardStatus, GroupRuntime, Job, Settings } from "./types";

type View = "overview" | "groups" | "jobs" | "settings" | "logs";
type SystemAction = "start" | "stop" | "restart";
type SessionInfo = {
  ok: boolean;
  mode: "local" | "remote";
  identity: { email: string } | null;
  csrf_token: string | null;
  permissions: string[];
};

let activeCsrfToken: string | null = null;

class ApiError extends Error {
  status: number;
  payload: Record<string, unknown>;

  constructor(status: number, payload: Record<string, unknown>) {
    super(String(payload.detail ?? payload.reason ?? `HTTP ${status}`));
    this.status = status;
    this.payload = payload;
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
  if (!response.ok) throw new ApiError(response.status, payload);
  return payload as T;
}

function formatTime(value: number | null | undefined): string {
  if (!value) return "—";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value * 1000));
}

function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let size = value;
  let index = -1;
  do {
    size /= 1024;
    index += 1;
  } while (size >= 1024 && index < units.length - 1);
  return `${size.toFixed(size >= 10 ? 1 : 2)} ${units[index]}`;
}

const statusText: Record<string, string> = {
  idle: "空闲",
  running: "进行中",
  caught_up: "已追平",
  catching_up: "补抓中",
  completed: "已完成",
  queued: "等待执行",
  cancelled: "已取消",
  failed: "失败",
  error: "异常",
  paused: "已暂停",
};

function StatusPill({ value, good }: { value: string | null; good?: boolean }) {
  const normalized = value ?? "idle";
  const tone = good || ["caught_up", "completed"].includes(normalized)
    ? "good"
    : ["failed", "error"].includes(normalized)
      ? "bad"
      : ["running", "catching_up", "queued"].includes(normalized)
        ? "active"
        : "neutral";
  return <span className={`pill ${tone}`}>{statusText[normalized] ?? normalized}</span>;
}

function ServiceCard({ title, state, detail }: { title: string; state?: { healthy: boolean; detail: string }; detail?: string }) {
  const healthy = Boolean(state?.healthy);
  return (
    <article className={`service-card ${healthy ? "online" : "offline"}`}>
      <div className="service-heading">
        <span className="service-dot" />
        <span>{title}</span>
      </div>
      <strong>{healthy ? "正常" : "未就绪"}</strong>
      <small title={detail ?? state?.detail}>{detail ?? state?.detail ?? "等待状态"}</small>
    </article>
  );
}

function Metric({ label, value, note }: { label: string; value: string | number; note?: string }) {
  return (
    <article className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
      {note && <small>{note}</small>}
    </article>
  );
}

function GroupRow({
  group,
  onBackfill,
  onDisable,
  onEnable,
}: {
  group: GroupRuntime;
  onBackfill: (groupId: string, mode: "page" | "continuous" | "rescan") => void;
  onDisable: (groupId: string) => void;
  onEnable: (groupId: string, displayName?: string) => void;
}) {
  return (
    <article className={`group-card ${group.enabled ? "" : "disabled"}`}>
      <div className="group-title">
        <div>
          <h3>{group.display_name || `群 ${group.group_id}`}</h3>
          <code>{group.group_id}</code>
        </div>
        <StatusPill value={group.enabled ? "completed" : "paused"} good={Boolean(group.enabled)} />
      </div>
      <div className="runtime-grid">
        <div>
          <span>新消息</span>
          <StatusPill value={group.recent_status} />
          <small>最后成功 {formatTime(group.recent_last_success)}</small>
          {group.recent_last_error && <small className="error-text">{group.recent_last_error}</small>}
        </div>
        <div>
          <span>历史回填</span>
          <StatusPill value={group.backfill_completed ? "completed" : group.backfill_status} />
          <small>当前最旧 {formatTime(group.backfill_cursor_time)}</small>
          {group.backfill_last_error && <small className="error-text">{group.backfill_last_error}</small>}
        </div>
      </div>
      <div className="group-counts">
        <span><b>{group.accepted}</b> 有效</span>
        <span><b>{group.rejected}</b> 淘汰</span>
        <span><b>{group.duplicates}</b> 重复</span>
        <span><b>{group.failed}</b> 失败</span>
      </div>
      <div className="button-row compact">
        {group.enabled ? (
          <>
            <button className="secondary" onClick={() => onBackfill(group.group_id, "page")}>回填一页</button>
            <button className="secondary" onClick={() => onBackfill(group.group_id, "continuous")}>连续回填</button>
            <button className="ghost" onClick={() => onBackfill(group.group_id, "rescan")}>重扫导入历史</button>
            <button className="danger-ghost" onClick={() => onDisable(group.group_id)}>停止监听</button>
          </>
        ) : (
          <button className="primary" onClick={() => onEnable(group.group_id, group.display_name ?? undefined)}>重新启用</button>
        )}
      </div>
    </article>
  );
}

function App() {
  const [view, setView] = useState<View>("overview");
  const [dashboard, setDashboard] = useState<DashboardStatus | null>(null);
  const [authorized, setAuthorized] = useState(false);
  const [session, setSession] = useState<SessionInfo | null>(null);
  const [authError, setAuthError] = useState<string | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const [confirmAction, setConfirmAction] = useState<SystemAction | null>(null);
  const [settings, setSettings] = useState<Settings | null>(null);
  const linuxMode = Boolean(
    settings?.external_services || settings?.deployment_mode === "linux-docker",
  );
  const [available, setAvailable] = useState<AvailableGroup[]>([]);
  const [groupSearch, setGroupSearch] = useState("");
  const [manualGroup, setManualGroup] = useState("");
  const [logs, setLogs] = useState<{ path: string; lines: string[] }[]>([]);
  const [migrationPath, setMigrationPath] = useState("");

  const notify = useCallback((message: string) => {
    setToast(message);
    window.setTimeout(() => setToast(null), 3500);
  }, []);

  const refresh = useCallback(async () => {
    const next = await api<DashboardStatus>("/api/v1/status");
    setDashboard(next);
  }, []);

  useEffect(() => {
    const establish = async () => {
      try {
        const params = new URLSearchParams(window.location.search);
        const token = params.get("session_token");
        let established: SessionInfo;
        if (token) {
          established = await api<SessionInfo>(`/api/v1/session?session_token=${encodeURIComponent(token)}`);
          window.history.replaceState({}, "", `${window.location.pathname}${window.location.hash}`);
        } else {
          established = await api<SessionInfo>("/api/v1/session");
        }
        activeCsrfToken = established.csrf_token;
        setSession(established);
        await refresh();
        setAuthorized(true);
      } catch (error) {
        setAuthError(error instanceof Error ? error.message : "无法建立控制台会话");
      }
    };
    void establish();
  }, [refresh]);

  useEffect(() => {
    if (!authorized) return;
    const source = new EventSource("/api/v1/events", { withCredentials: true });
    source.addEventListener("status", (event) => {
      setDashboard(JSON.parse((event as MessageEvent).data) as DashboardStatus);
    });
    const fallback = window.setInterval(() => void refresh().catch(() => undefined), 7000);
    return () => {
      source.close();
      window.clearInterval(fallback);
    };
  }, [authorized, refresh]);

  useEffect(() => {
    if (!authorized || view !== "settings") return;
    void api<Settings>("/api/v1/settings").then((value) => {
      setSettings(value);
      setMigrationPath(value.storage_root ?? "");
    }).catch((error: Error) => notify(error.message));
  }, [authorized, view, notify]);

  useEffect(() => {
    if (!authorized || view !== "logs" || session?.mode === "remote") return;
    void api<{ files: { path: string; lines: string[] }[] }>("/api/v1/logs?lines=250")
      .then((value) => setLogs(value.files))
      .catch((error: Error) => notify(error.message));
  }, [authorized, view, notify, session?.mode]);

  const requestSystem = async (action: SystemAction, confirmed = false) => {
    try {
      await api(`/api/v1/system/${action}`, {
        method: "POST",
        body: JSON.stringify({ confirm_close_qq: confirmed }),
      });
      setConfirmAction(null);
      notify(action === "stop" ? "正在安全停止采集 Worker" : "启动状态机已开始");
      await refresh();
    } catch (error) {
      if (error instanceof ApiError && error.status === 409 && error.payload.confirmation_required) {
        setConfirmAction(action);
        return;
      }
      notify(error instanceof Error ? error.message : "操作失败");
    }
  };

  const backfill = async (groupId: string, mode: "page" | "continuous" | "rescan") => {
    if (mode === "rescan" && !window.confirm("重新扫描会将该群历史游标移回当前时间并从头去重扫描。继续吗？")) return;
    try {
      await api(`/api/v1/groups/${groupId}/backfill`, {
        method: "POST",
        body: JSON.stringify({ mode }),
      });
      notify("回填任务已进入单 Worker 队列");
      await refresh();
    } catch (error) {
      notify(error instanceof Error ? error.message : "创建任务失败");
    }
  };

  const disableGroup = async (groupId: string) => {
    if (!window.confirm(`停止监听群 ${groupId}？图片、游标和记录都会保留。`)) return;
    try {
      await api(`/api/v1/groups/${groupId}`, { method: "DELETE" });
      notify("已停止监听，历史数据保留");
      await refresh();
    } catch (error) {
      notify(error instanceof Error ? error.message : "操作失败");
    }
  };

  const addGroup = async (groupId: string, displayName?: string) => {
    try {
      await api("/api/v1/groups", {
        method: "POST",
        body: JSON.stringify({ group_id: groupId.trim(), display_name: displayName || null }),
      });
      setManualGroup("");
      notify("监听群已加入，下一轮自动生效");
      await refresh();
    } catch (error) {
      notify(error instanceof Error ? error.message : "添加失败");
    }
  };

  const loadAvailable = async () => {
    try {
      const rows = await api<AvailableGroup[]>("/api/v1/groups/available");
      setAvailable(rows);
      notify(`已读取 ${rows.length} 个 QQ 群`);
    } catch (error) {
      notify(error instanceof Error ? error.message : "读取群列表失败");
    }
  };

  const saveSettings = async (event: FormEvent) => {
    event.preventDefault();
    if (!settings) return;
    try {
      const runtimePayload = {
        poll_interval_seconds: settings.poll_interval_seconds,
        catchup_page_size: settings.catchup_page_size,
        backfill_page_size: settings.backfill_page_size,
        collector_paused: settings.collector_paused,
        backfill_paused: settings.backfill_paused,
        deep_backfill_enabled: settings.deep_backfill_enabled,
      };
      const payload = session?.mode === "remote" || linuxMode
        ? {
            ...runtimePayload,
          }
        : settings;
      const updated = await api<Settings>("/api/v1/settings", {
        method: "PATCH",
        body: JSON.stringify(payload),
      });
      setSettings(updated);
      notify("设置已保存，轮询参数下一周期生效");
    } catch (error) {
      notify(error instanceof Error ? error.message : "保存失败");
    }
  };

  const startMigration = async () => {
    if (!migrationPath || migrationPath === settings?.storage_root) {
      notify("请输入一个不同的新仓库位置");
      return;
    }
    if (!window.confirm("迁移会暂停 Worker、复制并逐文件校验；旧仓库会保留作为回滚。现在开始吗？")) return;
    try {
      await api("/api/v1/storage/migrate", {
        method: "POST",
        body: JSON.stringify({ destination: migrationPath }),
      });
      notify("仓库迁移已开始，请勿关闭控制台");
      await refresh();
    } catch (error) {
      notify(error instanceof Error ? error.message : "迁移启动失败");
    }
  };

  const cancelJob = async (job: Job) => {
    try {
      await api(`/api/v1/jobs/${job.id}/cancel`, { method: "POST", body: "{}" });
      notify("将在当前页处理完成后安全取消");
      await refresh();
    } catch (error) {
      notify(error instanceof Error ? error.message : "取消失败");
    }
  };

  const completeSetup = async () => {
    try {
      await api("/api/v1/setup/complete", { method: "POST", body: "{}" });
      notify("首次配置已完成");
      await refresh();
    } catch (error) {
      notify(error instanceof Error ? error.message : "仍有配置项未完成");
    }
  };

  const filteredAvailable = useMemo(() => {
    const query = groupSearch.trim().toLowerCase();
    if (!query) return available;
    return available.filter((group) => `${group.group_id} ${group.group_name}`.toLowerCase().includes(query));
  }, [available, groupSearch]);

  if (authError) {
    return (
      <main className="center-screen">
        <div className="auth-card">
          <div className="brand-mark">AI</div>
          <h1>无法进入控制台</h1>
          <p>{authError}</p>
          <p className="muted">公网访问请重新完成 Cloudflare Access 登录；本机访问请从“QQ AI 原图采集控制台”快捷方式重新打开。</p>
        </div>
      </main>
    );
  }

  if (!dashboard) {
    return <main className="center-screen"><div className="loader" /><p>正在连接采集服务…</p></main>;
  }

  const activeAction = dashboard.action.status === "running";
  const workerRunning = dashboard.services.worker?.healthy;
  const accountName = dashboard.account
    ? `${dashboard.account.nickname || "QQ"} · ${dashboard.account.user_id}`
    : "尚未读取登录账号";
  const remoteMode = session?.mode === "remote";

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">AI</div>
          <div><strong>原图采集</strong><small>{remoteMode ? "SECURE REMOTE" : "LOCAL CONSOLE"}</small></div>
        </div>
        <nav>
          {([
            ["overview", "总览", "⌂"],
            ["groups", "监听群聊", "◎"],
            ["jobs", "回填任务", "↺"],
            ["settings", remoteMode ? "采集设置" : "设置与仓库", "⚙"],
            ...(!remoteMode ? [["logs", "运行日志", "≡"] as [View, string, string]] : []),
          ] as [View, string, string][]).map(([key, label, icon]) => (
            <button key={key} className={view === key ? "active" : ""} onClick={() => setView(key)}>
              <span>{icon}</span>{label}
            </button>
          ))}
        </nav>
        <div className="sidebar-footer">
          <span className={`live-dot ${workerRunning ? "on" : ""}`} />
          <div><strong>{workerRunning ? "采集中" : "采集已停止"}</strong><small>{accountName}</small></div>
        </div>
      </aside>

      <main className="content">
        <header className="topbar">
          <div>
            <p className="eyebrow">QQ AI IMAGE COLLECTOR</p>
            <h1>{view === "overview" ? "运行总览" : view === "groups" ? "监听群聊" : view === "jobs" ? "回填任务" : view === "settings" ? (remoteMode ? "采集设置" : "设置与仓库") : "运行日志"}</h1>
          </div>
          <div className="top-actions">
            <button className="secondary" disabled={activeAction} onClick={() => void requestSystem("restart")}>重启采集</button>
            {workerRunning ? (
              <button className="danger" disabled={activeAction} onClick={() => void requestSystem("stop")}>停止 Worker</button>
            ) : (
              <button className="primary" disabled={activeAction} onClick={() => void requestSystem("start")}>一键启动</button>
            )}
          </div>
        </header>

        {remoteMode && (
          <section className="remote-banner">
            <div><strong>Cloudflare Access 安全远程会话</strong><small>{session?.identity?.email}</small></div>
            <span>路径、仓库迁移与运行日志仅能在部署主机修改</span>
          </section>
        )}

        {!remoteMode && !dashboard.setup.completed && (
          <section className="setup-wizard">
            <div className="wizard-copy">
              <p className="eyebrow">FIRST RUN</p>
              <h2>首次配置向导</h2>
              <p>{linuxMode ? "依次启动 Docker 服务、扫码登录 QQ、检查 OneBot/QCE，并选择监听群。" : "依次确认依赖路径、启动 NapCat、读取群列表并选择仓库。工具只检测这些依赖，不会自动下载或替换现有版本。"}</p>
              <div className="wizard-actions">
                <button className="secondary" onClick={() => setView("settings")}>1 · {linuxMode ? "检查容器配置" : "配置路径与仓库"}</button>
                <button className="primary" onClick={() => void requestSystem("start")}>2 · 启动并检测</button>
                <button className="secondary" onClick={() => { setView("groups"); void loadAvailable(); }}>3 · 选择监听群</button>
                <button className="primary" disabled={!dashboard.setup.ready} onClick={() => void completeSetup()}>完成向导</button>
              </div>
            </div>
            <div className="check-list">
              {dashboard.setup.checks.map((check, index) => (
                <div key={check.key} className={check.ok ? "ok" : "missing"}>
                  <span>{check.ok ? "✓" : index + 1}</span>
                  <div><strong>{check.label}</strong><small title={check.detail}>{check.detail}</small></div>
                  {!check.ok && dashboard.setup.links[check.key] && <a href={dashboard.setup.links[check.key]} target="_blank" rel="noreferrer">官方说明 ↗</a>}
                </div>
              ))}
            </div>
          </section>
        )}

        {dashboard.action.status !== "idle" && (
          <section className={`action-banner ${dashboard.action.status}`}>
            <div className={dashboard.action.status === "running" ? "spinner" : "action-icon"}>
              {dashboard.action.status === "completed" ? "✓" : dashboard.action.status === "failed" ? "!" : ""}
            </div>
            <div>
              <strong>{dashboard.action.message || "系统操作"}</strong>
              <small>{dashboard.action.error || (dashboard.action.stage ? `阶段：${dashboard.action.stage}` : "")}</small>
            </div>
          </section>
        )}

        {dashboard.migration.status === "running" && (
          <section className="migration-banner">
            <div><strong>正在迁移仓库 · {dashboard.migration.stage}</strong><span>{dashboard.migration.current} / {dashboard.migration.total || "…"}</span></div>
            <progress value={dashboard.migration.current} max={dashboard.migration.total || 1} />
          </section>
        )}

        {view === "overview" && (
          <>
            <section className="account-strip">
              <div><span className="avatar">Q</span><div><small>当前登录 QQ</small><strong>{accountName}</strong></div></div>
              <span>状态刷新于 {formatTime(dashboard.timestamp)}</span>
            </section>
            <section className="service-grid">
              <ServiceCard title="管理服务" state={dashboard.services.manager} />
              <ServiceCard title="QQ" state={dashboard.services.qq} />
              <ServiceCard title={linuxMode ? "NapCat 服务" : "NapCat 注入"} state={dashboard.services.napcat} detail={`${dashboard.services.napcat?.detail ?? ""} · WebUI ${dashboard.services.webui?.healthy ? "正常" : "未就绪"}`} />
              <ServiceCard title="OneBot" state={dashboard.services.onebot} />
              <ServiceCard title="QCE" state={dashboard.services.qce} />
              <ServiceCard title="采集 Worker" state={dashboard.services.worker} />
            </section>
            <section className="section-heading"><div><p className="eyebrow">COLLECTION</p><h2>仓库统计</h2></div></section>
            <section className="metric-grid">
              <Metric label="去重有效图片" value={dashboard.statistics.unique_images} note={`${dashboard.statistics.accepted_records} 条有效记录`} />
              <Metric label="NovelAI" value={dashboard.statistics.novelai} />
              <Metric label="ComfyUI" value={dashboard.statistics.comfyui} />
              <Metric label="NAI含参但不可直接读取的" value={dashboard.statistics.novelai_unreadable} />
              <Metric label="其他模型生成" value={dashboard.statistics.other_models} />
              <Metric label="今日新增" value={dashboard.statistics.today_new} />
              <Metric label="图片占用" value={formatBytes(dashboard.statistics.disk_bytes)} />
              <Metric label="GIF 已排除" value={dashboard.statistics.gif_excluded} />
              <Metric label="来源已记录" value={dashboard.statistics.provenance_complete} note={`${dashboard.statistics.provenance_missing} 条待补`} />
              <Metric label="当前失败" value={dashboard.statistics.failed} note={`${dashboard.statistics.resolving} 个处理中`} />
            </section>
            <section className="section-heading"><div><p className="eyebrow">GROUPS</p><h2>群聊进度</h2></div><button className="text-button" onClick={() => setView("groups")}>管理全部 →</button></section>
            <section className="compact-table">
              {dashboard.groups.filter((group) => group.enabled).map((group) => (
                <div className="compact-row" key={group.group_id}>
                  <div><strong>{group.display_name || group.group_id}</strong><small>{group.group_id}</small></div>
                  <div><small>新消息</small><StatusPill value={group.recent_status} /></div>
                  <div><small>历史最旧</small><strong>{formatTime(group.backfill_cursor_time)}</strong></div>
                  <div><small>有效图片</small><strong>{group.accepted}</strong></div>
                </div>
              ))}
            </section>
          </>
        )}

        {view === "groups" && (
          <>
            <section className="panel add-panel">
              <div><p className="eyebrow">MONITOR TARGETS</p><h2>增加监听对象</h2><p>可从当前 QQ 群列表中查找，也可直接输入群号。重新启用不会清空原游标。</p></div>
              <form onSubmit={(event) => { event.preventDefault(); void addGroup(manualGroup); }}>
                <input value={manualGroup} onChange={(event) => setManualGroup(event.target.value.replace(/\D/g, ""))} placeholder="输入 QQ 群号" />
                <button className="primary" disabled={!manualGroup}>加入监听</button>
              </form>
              <button className="secondary" onClick={() => void loadAvailable()}>读取当前 QQ 群列表</button>
            </section>
            {available.length > 0 && (
              <section className="panel available-panel">
                <input value={groupSearch} onChange={(event) => setGroupSearch(event.target.value)} placeholder="搜索群名或群号" />
                <div className="available-list">
                  {filteredAvailable.slice(0, 100).map((group) => (
                    <div key={group.group_id}><div><strong>{group.group_name || "未命名群"}</strong><small>{group.group_id} · {group.member_count} 人</small></div><button className="secondary" onClick={() => void addGroup(group.group_id, group.group_name)}>监听</button></div>
                  ))}
                </div>
              </section>
            )}
            <section className="group-list">
              {dashboard.groups.map((group) => <GroupRow key={group.group_id} group={group} onBackfill={backfill} onDisable={disableGroup} onEnable={(groupId, name) => void addGroup(groupId, name)} />)}
            </section>
          </>
        )}

        {view === "jobs" && (
          <section className="panel">
            <div className="section-heading"><div><p className="eyebrow">SERIAL QUEUE</p><h2>手动回填队列</h2><p>每次只处理一页，然后把执行机会交回实时补漏。</p></div></div>
            <div className="job-table">
              <div className="job-row header"><span>任务</span><span>群号</span><span>状态</span><span>进度</span><span>更新时间</span><span /></div>
              {dashboard.jobs.map((job) => (
                <div className="job-row" key={job.id}>
                  <span>#{job.id} · {job.kind === "page" ? "单页" : job.kind === "continuous" ? "连续" : "重扫"}</span>
                  <code>{job.group_id}</code>
                  <StatusPill value={job.status} />
                  <span>{job.progress_pages} 页</span>
                  <span>{formatTime(job.updated_at)}</span>
                  <span>{["queued", "running"].includes(job.status) && <button className="danger-ghost" onClick={() => void cancelJob(job)}>安全取消</button>}</span>
                  {job.error && <small className="job-error">{job.error}</small>}
                </div>
              ))}
              {!dashboard.jobs.length && <div className="empty">暂无手动回填任务</div>}
            </div>
          </section>
        )}

        {view === "settings" && settings && (
          <>
            <form className="panel settings-form" onSubmit={(event) => void saveSettings(event)}>
              <div className="section-heading"><div><p className="eyebrow">RUNTIME</p><h2>{remoteMode ? "安全远程采集设置" : "运行与依赖"}</h2></div><button className="primary" type="submit">保存设置</button></div>
              <div className="form-grid">
                {!remoteMode && !linuxMode && <>
                  <label className="wide"><span>QQ.exe 路径</span><input value={settings.qq_path ?? ""} onChange={(event) => setSettings({ ...settings, qq_path: event.target.value })} /></label>
                  <label className="wide"><span>NapCat 根目录</span><input value={settings.napcat_root ?? ""} onChange={(event) => setSettings({ ...settings, napcat_root: event.target.value })} /></label>
                  <label><span>启动器类型</span><select value={settings.launcher_kind ?? "framework"} onChange={(event) => setSettings({ ...settings, launcher_kind: event.target.value as "framework" | "shell" })}><option value="framework">Framework</option><option value="shell">官方 Shell</option></select></label>
                  <label><span>Shell 启动器</span><input value={settings.shell_launcher ?? ""} onChange={(event) => setSettings({ ...settings, shell_launcher: event.target.value || null })} placeholder="仅 Shell 模式需要" /></label>
                </>}
                {!remoteMode && linuxMode && <label className="wide"><span>部署方式</span><input value="Linux Docker · NapCat/QCE 由 Compose 管理" readOnly /></label>}
                <label><span>轮询间隔（秒）</span><input type="number" min={15} max={3600} value={settings.poll_interval_seconds} onChange={(event) => setSettings({ ...settings, poll_interval_seconds: Number(event.target.value) })} /></label>
                <label><span>新消息页大小</span><input type="number" min={20} max={500} value={settings.catchup_page_size} onChange={(event) => setSettings({ ...settings, catchup_page_size: Number(event.target.value) })} /></label>
                <label><span>历史页大小</span><input type="number" min={2} max={500} value={settings.backfill_page_size} onChange={(event) => setSettings({ ...settings, backfill_page_size: Number(event.target.value) })} /></label>
              </div>
              <div className="toggle-grid">
                <label><input type="checkbox" checked={settings.collector_paused} onChange={(event) => setSettings({ ...settings, collector_paused: event.target.checked })} /><span><strong>暂停全部采集</strong><small>Worker 保持运行但不下载</small></span></label>
                <label><input type="checkbox" checked={settings.backfill_paused} onChange={(event) => setSettings({ ...settings, backfill_paused: event.target.checked })} /><span><strong>暂停自动历史回填</strong><small>实时补漏仍继续</small></span></label>
                <label><input type="checkbox" checked={settings.deep_backfill_enabled ?? false} onChange={(event) => setSettings({ ...settings, deep_backfill_enabled: event.target.checked })} /><span><strong>深层漫游扩展</strong><small>{linuxMode ? "仅在 QCE 提供 fetch-before 扩展时启用" : "继续追溯漫游历史"}</small></span></label>
              </div>
            </form>
            {!remoteMode && !linuxMode && <section className="panel storage-panel">
              <div><p className="eyebrow">STORAGE</p><h2>图片仓库</h2><p>当前：<code>{settings.storage_root}</code></p></div>
              <div className="button-row"><button className="secondary" onClick={() => void api("/api/v1/storage/open", { method: "POST", body: JSON.stringify({}) }).then(() => notify("已打开仓库")).catch((error: Error) => notify(error.message))}>打开文件夹</button></div>
              <div className="migration-box">
                <label><span>迁移到新位置</span><input value={migrationPath} onChange={(event) => setMigrationPath(event.target.value)} placeholder="例如 E:\\QQ-AI-Images" /></label>
                <button className="danger" disabled={dashboard.migration.status === "running"} onClick={() => void startMigration()}>校验并迁移</button>
              </div>
              <small className="muted">迁移会先停止 Worker、备份 SQLite、检查空间、复制并逐文件校验 SHA-256；成功后旧文件仍保留。</small>
            </section>}
            {!remoteMode && linuxMode && <section className="panel storage-panel">
              <div><p className="eyebrow">STORAGE</p><h2>Linux 持久化仓库</h2><p>容器内：<code>{settings.storage_root}</code></p></div>
              <small className="muted">宿主机目录由 docker-compose.yml 的只读/读写卷映射管理。请使用迁移脚本修改宿主机仓库位置。</small>
            </section>}
          </>
        )}

        {view === "logs" && (
          <section className="panel logs-panel">
            <div className="section-heading"><div><p className="eyebrow">DIAGNOSTICS</p><h2>最近运行日志</h2></div><button className="secondary" onClick={() => void api<{ files: { path: string; lines: string[] }[] }>("/api/v1/logs?lines=250").then((value) => setLogs(value.files))}>刷新</button></div>
            {logs.map((file) => <div className="log-file" key={file.path}><strong>{file.path}</strong><pre>{file.lines.join("\n") || "（空）"}</pre></div>)}
            {!logs.length && <div className="empty">尚未生成控制台 Worker 日志</div>}
          </section>
        )}
      </main>

      {confirmAction && (
        <div className="modal-backdrop">
          <div className="modal">
            <div className="warning-mark">!</div>
            <h2>需要关闭当前普通 QQ</h2>
            <p>当前 QQ 没有有效的 NapCat 注入。确认后会先请求 QQ 正常退出；10 秒仍未退出才结束残留进程，然后通过 NapCat 启动。</p>
            <div className="button-row"><button className="secondary" onClick={() => setConfirmAction(null)}>取消</button><button className="danger" onClick={() => void requestSystem(confirmAction, true)}>确认关闭并继续</button></div>
          </div>
        </div>
      )}
      {toast && <div className="toast">{toast}</div>}
    </div>
  );
}

export default App;
