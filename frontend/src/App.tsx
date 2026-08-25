import { useCallback, useEffect, useMemo, useState } from "react";
import type { AvailableGroup, DashboardStatus, GroupRuntime, ModelStats, Settings } from "./types";

type View = "overview" | "groups" | "settings" | "logs";
type SessionInfo = { mode: "local" | "remote" | "direct"; identity: { email: string } | null };

/** A group with no image for this long is shown as dormant rather than active. */
const DORMANT_AFTER_SECONDS = 3 * 3600;

async function api<T>(url: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(url, {
    credentials: "include",
    headers: { "Content-Type": "application/json", ...(options.headers ?? {}) },
    ...options,
  });
  if (!response.ok) {
    const detail = await response.json().catch(() => ({ detail: response.statusText }));
    throw new Error(detail?.detail ?? `请求失败：${response.status}`);
  }
  return response.status === 204 ? (undefined as T) : ((await response.json()) as T);
}

function ago(value: number | null | undefined): string {
  if (!value) return "从未";
  const seconds = Math.max(0, Math.floor(Date.now() / 1000) - value);
  if (seconds < 60) return "刚刚";
  if (seconds < 3600) return `${Math.floor(seconds / 60)} 分钟前`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)} 小时前`;
  return `${Math.floor(seconds / 86400)} 天前`;
}

function bytes(value: number): string {
  if (!value) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(units.length - 1, Math.floor(Math.log(value) / Math.log(1024)));
  return `${(value / 1024 ** index).toFixed(index >= 3 ? 2 : 0)} ${units[index]}`;
}

const n = (value: number) => value.toLocaleString("en-US");

const CATEGORIES = [
  { key: "novelai", label: "NovelAI", tone: "nai" },
  { key: "comfyui", label: "ComfyUI", tone: "comfy" },
  { key: "other_models", label: "其他模型", tone: "other" },
  { key: "novelai_unreadable", label: "NAI 不可读", tone: "unreadable" },
] as const;

/** Only these services are worth interrupting the user for. */
const CRITICAL_SERVICES = ["event_stream", "worker", "downloader", "queue", "onebot", "qq"];

export default function App() {
  const [view, setView] = useState<View>("overview");
  const [dashboard, setDashboard] = useState<DashboardStatus | null>(null);
  const [authorized, setAuthorized] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const [settings, setSettings] = useState<Settings | null>(null);
  const [available, setAvailable] = useState<AvailableGroup[]>([]);
  const [search, setSearch] = useState("");
  const [manualGroup, setManualGroup] = useState("");
  const [logs, setLogs] = useState<{ path: string; lines: string[] }[]>([]);
  const [showDetail, setShowDetail] = useState(false);

  const notify = useCallback((message: string) => {
    setToast(message);
    window.setTimeout(() => setToast(null), 4000);
  }, []);

  const refresh = useCallback(async () => setDashboard(await api<DashboardStatus>("/api/v1/status")), []);

  useEffect(() => {
    void (async () => {
      const token = new URLSearchParams(window.location.search).get("session_token");
      try {
        await api<SessionInfo>(
          token ? `/api/v1/session?session_token=${encodeURIComponent(token)}` : "/api/v1/session",
        );
        if (token) window.history.replaceState({}, "", window.location.pathname);
        setAuthorized(true);
        await refresh();
      } catch (error) {
        setAuthError((error as Error).message);
      }
    })();
  }, [refresh]);

  useEffect(() => {
    if (!authorized) return;
    const source = new EventSource("/api/v1/events", { withCredentials: true });
    source.onmessage = (event) => {
      try {
        setDashboard(JSON.parse(event.data) as DashboardStatus);
      } catch {
        /* keep the last good snapshot */
      }
    };
    const timer = window.setInterval(() => void refresh().catch(() => undefined), 30000);
    return () => {
      source.close();
      window.clearInterval(timer);
    };
  }, [authorized, refresh]);

  useEffect(() => {
    if (!authorized || view !== "settings") return;
    void api<Settings>("/api/v1/settings").then(setSettings).catch((e: Error) => notify(e.message));
  }, [authorized, view, notify]);

  useEffect(() => {
    if (!authorized || view !== "logs") return;
    void api<{ files: { path: string; lines: string[] }[] }>("/api/v1/logs?lines=250")
      .then((value) => setLogs(value.files))
      .catch((e: Error) => notify(e.message));
  }, [authorized, view, notify]);

  const run = useCallback(
    async (label: string, task: () => Promise<unknown>) => {
      try {
        await task();
        notify(label);
        await refresh();
      } catch (error) {
        notify((error as Error).message);
      }
    },
    [notify, refresh],
  );

  const stats = dashboard?.statistics;
  const today = stats?.today;

  const problems = useMemo(() => {
    if (!dashboard) return [] as { key: string; detail: string }[];
    const found = CRITICAL_SERVICES.filter((key) => dashboard.services[key] && !dashboard.services[key].healthy).map(
      (key) => ({ key, detail: dashboard.services[key].detail }),
    );
    if ((today?.get_image_blocked ?? 0) > 0) {
      found.unshift({ key: "安全", detail: `拦截到 ${today?.get_image_blocked} 次 get_image 调用，采集已暂停` });
    }
    if (dashboard.action?.status === "failed") {
      found.push({ key: "操作", detail: dashboard.action.error ?? "上次操作失败" });
    }
    // A frozen snapshot looks exactly like a healthy but quiet pipeline, so it
    // has to be called out explicitly rather than inferred from the numbers.
    const age = dashboard.snapshot_age_seconds ?? 0;
    if (dashboard.snapshot_starting) {
      return [{ key: "正在启动", detail: "控制台正在读取首次状态，稍候自动刷新" }];
    }
    if (age > 120) {
      found.unshift({
        key: "数据陈旧",
        detail: `页面显示的是 ${Math.floor(age / 60)} 分钟前的快照，后台状态刷新可能已卡死`,
      });
    }
    return found;
  }, [dashboard, today]);

  const lastImageAt = useMemo(
    () => (dashboard?.groups ?? []).reduce((max, g) => Math.max(max, g.last_image_at ?? 0), 0),
    [dashboard],
  );

  const sortedGroups = useMemo(
    () => [...(dashboard?.groups ?? [])].sort((a, b) => (b.last_image_at ?? 0) - (a.last_image_at ?? 0)),
    [dashboard],
  );

  if (authError) {
    return (
      <div className="gate">
        <h1>无法建立本地会话</h1>
        <p>{authError}</p>
        <p className="muted">请通过 <code>./manage.sh console-url</code> 获取带令牌的地址。</p>
      </div>
    );
  }

  if (!dashboard || !stats || !today) {
    return <div className="gate"><h1>正在读取采集状态…</h1></div>;
  }

  const totalCategorised = CATEGORIES.reduce((sum, c) => sum + (stats[c.key] as number), 0) || 1;
  const paused = settings?.collector_paused ?? false;

  return (
    <div className="app">
      <StatusBar
        problems={problems}
        lastImageAt={lastImageAt}
        account={dashboard.account}
        queueDepth={stats.queue.depth}
      />

      <nav className="tabs">
        {([["overview", "概览"], ["groups", "群聊"], ["settings", "设置"], ["logs", "日志"]] as [View, string][]).map(
          ([key, label]) => (
            <button key={key} className={view === key ? "tab active" : "tab"} onClick={() => setView(key)}>
              {label}
            </button>
          ),
        )}
        <span className="spacer" />
        <button className="ghost" onClick={() => void run("已刷新", refresh)}>刷新</button>
      </nav>

      {view === "overview" && (
        <>
          <section className="hero">
            <div className="hero-main">
              <div className="hero-number">{n(stats.unique_images)}</div>
              <div className="hero-label">张含参数的图片已收藏</div>
            </div>
            <div className="hero-side">
              <div className="hero-delta">
                <strong>+{n(today.accepted)}</strong>
                <span>今天新增</span>
              </div>
              <div className="hero-meta">
                <div><strong>{bytes(stats.disk_bytes)}</strong><span>仓库大小</span></div>
                <div><strong>{ago(lastImageAt)}</strong><span>最后一张</span></div>
              </div>
            </div>
          </section>

          <section className="panel">
            <div className="bar">
              {CATEGORIES.map((c) => {
                const value = stats[c.key] as number;
                return value > 0 ? (
                  <span
                    key={c.key}
                    className={`seg ${c.tone}`}
                    style={{ width: `${(value / totalCategorised) * 100}%` }}
                    title={`${c.label} ${n(value)}`}
                  />
                ) : null;
              })}
            </div>
            <div className="legend">
              {CATEGORIES.map((c) => {
                const value = stats[c.key] as number;
                return (
                  <div key={c.key} className="legend-item">
                    <i className={`dot ${c.tone}`} />
                    <span className="legend-label">{c.label}</span>
                    <strong>{n(value)}</strong>
                    <span className="muted">{((value / totalCategorised) * 100).toFixed(1)}%</span>
                  </div>
                );
              })}
            </div>
          </section>

          <ModelPanel models={stats.models} />

          <section className="pulse">
            <Pulse label="队列待下载" value={n(stats.queue.depth)} warn={stats.queue.depth > 500} />
            <Pulse label="今日收到消息" value={n(today.events)} />
            <Pulse label="今日图片" value={n(today.images_seen)} />
            <Pulse label="今日淘汰" value={n(today.rejected + today.filtered_gif)} muted />
          </section>

          <section className="panel">
            <div className="panel-head">
              <h2>各群产出</h2>
              <span className="muted">按最后收图时间排序</span>
            </div>
            <GroupTable groups={sortedGroups} />
          </section>

          <details className="detail" open={showDetail} onToggle={(e) => setShowDetail(e.currentTarget.open)}>
            <summary>技术细节</summary>
            <div className="grid">
              <Detail label="CDN 请求" value={n(today.cdn_requests)} />
              <Detail label="CDN 下载" value={n(today.cdn_downloads)} />
              <Detail label="CDN 流量" value={bytes(today.cdn_bytes)} />
              <Detail label="CDN 400" value={n(today.cdn_400)} warn={today.cdn_400 > 0} />
              <Detail label="CDN 403 / 429" value={`${today.cdn_403} / ${today.cdn_429}`} warn={today.cdn_403 + today.cdn_429 > 0} />
              <Detail label="历史调用（今日）" value={n(today.history_calls)} />
              <Detail label="get_image 拦截" value={n(today.get_image_blocked)} warn={today.get_image_blocked > 0} />
              <Detail label="重复图片" value={n(today.duplicates)} />
              <Detail label="失败 / 过期" value={`${n(today.failed)} / ${n(today.expired)}`} />
              <Detail label="累计记录" value={n(stats.accepted_records)} />
            </div>
            <p className="note">
              稳态下「历史调用」应保持在低位，「get_image 拦截」必须为 0。前者受每小时与每日预算限制，
              触顶时补漏会自动挂起而不是继续请求。
            </p>
          </details>
        </>
      )}

      {view === "groups" && (
        <>
          <section className="panel">
            <div className="panel-head"><h2>正在监听（{dashboard.groups.length}）</h2></div>
            <GroupTable
              groups={sortedGroups}
              onDisable={(id) => void run("已停止监听", () => api(`/api/v1/groups/${id}`, { method: "DELETE" }))}
              onRecover={(id) =>
                void run("已排入补漏", () =>
                  api(`/api/v1/groups/${id}/recover-gap`, { method: "POST", body: "{}" }),
                )
              }
            />
          </section>

          <section className="panel">
            <div className="panel-head">
              <h2>添加群聊</h2>
              <button
                className="ghost"
                onClick={() =>
                  void run("已读取群列表", async () => setAvailable(await api<AvailableGroup[]>("/api/v1/groups/available")))
                }
              >
                读取账号群列表
              </button>
            </div>
            <div className="row">
              <input placeholder="手动输入群号" value={manualGroup} onChange={(e) => setManualGroup(e.target.value)} />
              <button
                className="primary"
                disabled={!manualGroup.trim()}
                onClick={() =>
                  void run("已加入监听", async () => {
                    await api("/api/v1/groups", {
                      method: "POST",
                      body: JSON.stringify({ group_id: manualGroup.trim(), display_name: null }),
                    });
                    setManualGroup("");
                  })
                }
              >
                添加
              </button>
            </div>
            {available.length > 0 && (
              <>
                <input
                  className="search"
                  placeholder="搜索群名或群号"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                />
                <div className="available">
                  {available
                    .filter((g) => !search || g.group_name.includes(search) || g.group_id.includes(search))
                    .slice(0, 60)
                    .map((g) => {
                      const joined = dashboard.groups.some((row) => row.group_id === g.group_id);
                      return (
                        <div key={g.group_id} className="available-row">
                          <span className="name">{g.group_name}</span>
                          <span className="muted">{g.group_id} · {g.member_count} 人</span>
                          <button
                            className="ghost"
                            disabled={joined}
                            onClick={() =>
                              void run("已加入监听", () =>
                                api("/api/v1/groups", {
                                  method: "POST",
                                  body: JSON.stringify({ group_id: g.group_id, display_name: g.group_name }),
                                }),
                              )
                            }
                          >
                            {joined ? "监听中" : "添加"}
                          </button>
                        </div>
                      );
                    })}
                </div>
              </>
            )}
          </section>
        </>
      )}

      {view === "settings" && settings && (
        <section className="panel">
          <div className="panel-head"><h2>采集设置</h2></div>
          <label className="toggle">
            <input
              type="checkbox"
              checked={paused}
              onChange={(e) =>
                void run(e.target.checked ? "已暂停采集" : "已恢复采集", async () =>
                  setSettings(
                    await api<Settings>("/api/v1/settings", {
                      method: "PATCH",
                      body: JSON.stringify({ collector_paused: e.target.checked }),
                    }),
                  ),
                )
              }
            />
            <span>
              <strong>暂停采集</strong>
              <small>Worker 保持运行，但不再下载新图片</small>
            </span>
          </label>
          <div className="grid">
            <Detail label="仓库位置" value={settings.storage_root ?? "—"} />
            <Detail label="URL 来源" value={settings.url_preference === "data" ? "消息段 data.url" : "原始 originImageUrl"} />
            <Detail label="下载间隔" value={`${settings.download_interval_seconds}s ± ${settings.download_jitter_seconds}s`} />
          </div>
          <div className="row">
            <button className="ghost" onClick={() => void run("已请求启动", () => api("/api/v1/system/start", { method: "POST", body: "{}" }))}>启动</button>
            <button className="ghost" onClick={() => void run("已请求停止", () => api("/api/v1/system/stop", { method: "POST", body: "{}" }))}>停止</button>
            <button className="ghost" onClick={() => void run("已请求重启", () => api("/api/v1/system/restart", { method: "POST", body: "{}" }))}>重启</button>
          </div>
        </section>
      )}

      {view === "logs" && (
        <section className="panel">
          <div className="panel-head"><h2>日志</h2></div>
          {logs.map((file) => (
            <div key={file.path} className="log">
              <h3>{file.path}</h3>
              <pre>{file.lines.join("\n")}</pre>
            </div>
          ))}
          {logs.length === 0 && <p className="muted">暂无日志。</p>}
        </section>
      )}

      {toast && <div className="toast">{toast}</div>}
    </div>
  );
}

function StatusBar({
  problems,
  lastImageAt,
  account,
  queueDepth,
}: {
  problems: { key: string; detail: string }[];
  lastImageAt: number;
  account: { user_id: string; nickname: string } | null;
  queueDepth: number;
}) {
  const healthy = problems.length === 0;
  return (
    <header className={healthy ? "status ok" : "status bad"}>
      <span className="light" />
      <div className="status-text">
        {healthy ? (
          <>
            <strong>采集正常运行</strong>
            <span className="muted">
              最后收图 {ago(lastImageAt)}
              {queueDepth > 0 ? ` · 队列 ${queueDepth}` : ""}
            </span>
          </>
        ) : (
          <>
            <strong>{problems.length} 项异常需要处理</strong>
            <span className="muted">{problems.map((p) => `${p.key}：${p.detail}`).join("；")}</span>
          </>
        )}
      </div>
      {account && <span className="account">{account.nickname} · {account.user_id}</span>}
    </header>
  );
}

function GroupTable({
  groups,
  onDisable,
  onRecover,
}: {
  groups: GroupRuntime[];
  onDisable?: (id: string) => void;
  onRecover?: (id: string) => void;
}) {
  const now = Math.floor(Date.now() / 1000);
  return (
    <div className="table">
      <div className="tr th">
        <span>群聊</span>
        <span className="num">今日</span>
        <span className="num">累计收藏</span>
        <span className="num">队列</span>
        <span>最后收图</span>
        {(onDisable || onRecover) && <span />}
      </div>
      {groups.map((g) => {
        const dormant = !g.last_image_at || now - g.last_image_at > DORMANT_AFTER_SECONDS;
        return (
          <div key={g.group_id} className={dormant ? "tr dormant" : "tr"}>
            <span className="name">
              <i className={dormant ? "dot idle" : "dot live"} />
              {g.display_name || g.group_id}
              <small className="muted">{g.group_id}</small>
            </span>
            <span className="num today">{g.accepted_today > 0 ? `+${n(g.accepted_today)}` : "—"}</span>
            <span className="num strong">{n(g.accepted)}</span>
            <span className="num">{g.queued > 0 ? n(g.queued) : "—"}</span>
            <span className={dormant ? "muted" : ""}>{ago(g.last_image_at)}</span>
            {(onDisable || onRecover) && (
              <span className="actions">
                {onRecover && <button className="ghost sm" onClick={() => onRecover(g.group_id)}>补漏</button>}
                {onDisable && <button className="ghost sm danger" onClick={() => onDisable(g.group_id)}>停止</button>}
              </span>
            )}
          </div>
        );
      })}
      {groups.length === 0 && <p className="muted pad">尚未监听任何群聊。</p>}
    </div>
  );
}

const FAMILY_TONE: Record<string, string> = {
  "NAI-V5": "nai5", "NAI-V4": "nai", ComfyUI: "comfy",
  其他模型生成: "other", NovelAI: "unreadable",
};

function ModelPanel({ models }: { models: ModelStats }) {
  if (!models?.available) {
    return (
      <section className="panel">
        <div className="panel-head"><h2>模型分布</h2></div>
        <p className="muted">模型索引尚未建立。</p>
      </section>
    );
  }
  const total = models.total.reduce((sum, r) => sum + r.count, 0) || 1;
  const dayMap = (rows: { family: string; count: number }[]) =>
    Object.fromEntries(rows.map((r) => [r.family, r.count]));
  const today = dayMap(models.days.today);
  const yest = dayMap(models.days.yesterday);
  return (
    <section className="panel">
      <div className="panel-head">
        <h2>模型分布</h2>
        <span className="muted">已索引 {n(models.indexed ?? total)} 张 · 每 15 分钟增量更新</span>
      </div>
      <div className="bar">
        {models.total.map((r) => (
          <span key={r.family} className={`seg ${FAMILY_TONE[r.family] ?? "unreadable"}`}
                style={{ width: `${(r.count / total) * 100}%` }} title={`${r.family} ${n(r.count)}`} />
        ))}
      </div>
      <div className="table model-table">
        <div className="tr th">
          <span>模型</span>
          <span className="num">累计</span>
          <span className="num">占比</span>
          <span className="num">昨日</span>
          <span className="num">今日</span>
        </div>
        {models.total.map((r) => (
          <div key={r.family} className="tr">
            <span className="name">
              <i className={`dot ${FAMILY_TONE[r.family] ?? "unreadable"}`} />{r.family}
            </span>
            <span className="num strong">{n(r.count)}</span>
            <span className="num muted">{((r.count / total) * 100).toFixed(1)}%</span>
            <span className="num muted">{yest[r.family] ? n(yest[r.family]) : "—"}</span>
            <span className="num today">{today[r.family] ? `+${n(today[r.family])}` : "—"}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function Pulse({ label, value, warn, muted }: { label: string; value: string; warn?: boolean; muted?: boolean }) {
  return (
    <div className={`pulse-item${warn ? " warn" : ""}${muted ? " dim" : ""}`}>
      <strong>{value}</strong>
      <span>{label}</span>
    </div>
  );
}

function Detail({ label, value, warn }: { label: string; value: string; warn?: boolean }) {
  return (
    <div className={warn ? "detail-item warn" : "detail-item"}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}
