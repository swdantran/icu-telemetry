import { useEffect, useState } from "react";

type Vitals = { patient_id: string; hr: number; spo2: number; bp_sys: number; bp_dia: number; resp_rate: number; time: string };
type Alert = { patient_id: string; severity: string; rule: string; detail: string; time: string };

export default function App() {
  const [patients, setPatients] = useState<Record<string, Vitals>>({});
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [alertFlash, setAlertFlash] = useState<Record<string, number>>({});

  useEffect(() => {
    let ws: WebSocket;
    let retry: number;
    const connect = () => {
      ws = new WebSocket("ws://localhost:8001/ws");
      ws.onmessage = (ev) => {
        const msg = JSON.parse(ev.data);
        if (msg.type === "vitals") {
          setPatients((p) => ({ ...p, [msg.data.patient_id]: msg.data }));
        } else if (msg.type === "alert") {
          setAlerts((a) => {
            const k = (x: Alert) => x.time + x.patient_id + x.rule;
            if (a.some((x) => k(x) === k(msg.data))) return a;
            return [msg.data, ...a].slice(0, 20);
          });
          setAlertFlash((f) => ({ ...f, [msg.data.patient_id]: Date.now() }));
        }
      };
      ws.onclose = () => { retry = setTimeout(connect, 2000); };
    };
    connect();
    return () => { clearTimeout(retry); ws?.close(); };
  }, []);

  // eslint-disable-next-line react-hooks/purity
  const now = Date.now();
  const isAlerting = (pid: string) =>
    now - (alertFlash[pid] ?? 0) < 30_000;

  return (
    <div style={{ fontFamily: "system-ui", background: "#0f172a", minHeight: "100vh", color: "#e2e8f0", padding: 24 }}>
      <h1 style={{ margin: "0 0 16px" }}>ICU Ward - Live Telemetry <span style={{ fontSize: 14, color: "#64748b" }}>(simulated)</span></h1>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))", gap: 16 }}>
        {Object.values(patients).map((v) => (
          <div key={v.patient_id} style={{
            background: isAlerting(v.patient_id) ? "#7f1d1d" : "#1e293b",
            border: isAlerting(v.patient_id) ? "2px solid #ef4444" : "2px solid #334155",
            borderRadius: 12, padding: 16, transition: "all .3s",
          }}>
            <div style={{ fontSize: 18, fontWeight: 700, marginBottom: 8 }}>{v.patient_id.toUpperCase()}</div>
            <div style={{ fontSize: 28, fontWeight: 800 }}>{v.hr.toFixed(0)} <span style={{ fontSize: 13, color: "#94a3b8" }}>bpm</span></div>
            <div>SpO₂ <b style={{ color: v.spo2 < 93 ? "#f87171" : "#4ade80" }}>{v.spo2.toFixed(1)}%</b></div>
            <div>BP {v.bp_sys.toFixed(0)}/{v.bp_dia.toFixed(0)} · RR {v.resp_rate.toFixed(0)}</div>
          </div>
        ))}
      </div>
      <h2 style={{ marginTop: 28 }}>Alerts</h2>
      <div>
        {alerts.length === 0 && <div style={{ color: "#64748b" }}>No alerts yet — all patients stable.</div>}
        {alerts.map((a, i) => (
          <div key={i} style={{ padding: "8px 12px", marginBottom: 6, borderRadius: 8, background: a.severity === "critical" ? "#7f1d1d" : "#78350f" }}>
            <b>{a.severity.toUpperCase()}</b> · {a.patient_id} · {a.detail}
          </div>
        ))}
      </div>
    </div>
  );
}