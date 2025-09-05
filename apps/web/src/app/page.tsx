"use client";
import { useEffect, useState } from "react";
import { supabase } from "../lib/supabase";

type RowMM = { make: string; model: string; tests: number; fails: number; fail_rate_pct: number; };

export default function Page() {
  const [rows, setRows] = useState<RowMM[]>([]);
  const [make, setMake] = useState<string>("");

  useEffect(() => {
    (async () => {
      const { data, error } = await supabase
        .from("v_failure_rate_by_model")
        .select("*")
        .limit(100);
      if (!error && data) setRows(data as any);
    })();
  }, []);

  const filtered = make ? rows.filter(r => r.make.toLowerCase().includes(make.toLowerCase())) : rows;

  return (
    <main>
      <h1 style={{ fontSize: 28, marginBottom: 16 }}>MOT Failure Explorer</h1>
      <p style={{ opacity: 0.8, marginBottom: 24 }}>
        Explore average MOT failure rates by make/model (sample data). Type a make to filter.
      </p>

      <input
        placeholder="Filter by make (e.g., Ford)"
        value={make}
        onChange={e => setMake(e.target.value)}
        style={{ padding: 10, borderRadius: 8, border: "1px solid #374151", background: "#111827", color: "#e5e7eb", marginBottom: 16 }}
      />

      <div style={{ overflowX: "auto", borderRadius: 12, border: "1px solid #1f2937" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead style={{ background: "#111827" }}>
            <tr>
              <th style={th}>Make</th>
              <th style={th}>Model</th>
              <th style={th}>Tests</th>
              <th style={th}>Fails</th>
              <th style={th}>Fail Rate %</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((r, i) => (
              <tr key={i} style={{ borderTop: "1px solid #1f2937" }}>
                <td style={td}>{r.make}</td>
                <td style={td}>{r.model}</td>
                <td style={td}>{r.tests.toLocaleString()}</td>
                <td style={td}>{r.fails.toLocaleString()}</td>
                <td style={td}>{r.fail_rate_pct.toFixed(2)}</td>
              </tr>
            ))}
            {filtered.length === 0 && (
              <tr><td colSpan={5} style={{ padding: 16, textAlign: "center", opacity: 0.7 }}>No results.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </main>
  );
}

const th: React.CSSProperties = { textAlign: "left", padding: "12px 14px", fontWeight: 600 };
const td: React.CSSProperties = { padding: "12px 14px" };
