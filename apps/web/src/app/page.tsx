"use client";
import { useEffect, useState } from "react";
import { supabase } from "../lib/supabase";
import {
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
} from "recharts";

type RowMM = { make: string; model: string; tests: number; fails: number; fail_rate_pct: number };
type RowYear = { first_use_year: number; tests: number; fail_rate_pct: number };
type RowReason = { code: string; description: string | null; occurrences: number };

export default function Page() {
  const [rows, setRows] = useState<RowMM[]>([]);
  const [yearRows, setYearRows] = useState<RowYear[]>([]);
  const [reasons, setReasons] = useState<RowReason[]>([]);
  const [make, setMake] = useState<string>("");

  useEffect(() => {
    (async () => {
      const { data: models } = await supabase
        .from("v_failure_rate_by_model")
        .select("*")
        .limit(100);

      const { data: years } = await supabase
        .from("v_failure_rate_by_year")
        .select("*")
        .order("first_use_year", { ascending: true });

      const { data: top } = await supabase
        .from("v_top_failure_reasons")
        .select("*");

      if (models) setRows(models as RowMM[]);
      if (years) setYearRows(years as RowYear[]);
      if (top) setReasons(top as RowReason[]);
    })();
  }, []);

  const filtered = make
    ? rows.filter((r) => r.make.toLowerCase().includes(make.toLowerCase()))
    : rows;

  return (
    <main>
      <h1 style={{ fontSize: 28, marginBottom: 16 }}>MOT Failure Explorer</h1>
      <p style={{ opacity: 0.8, marginBottom: 24 }}>
        Explore MOT failure rates by make/model, vehicle age, and top failure reasons. Type a make to filter.
      </p>

      {/* Filter + Table */}
      <input
        placeholder="Filter by make (e.g., Ford)"
        value={make}
        onChange={(e) => setMake(e.target.value)}
        style={{
          padding: 10,
          borderRadius: 8,
          border: "1px solid #374151",
          background: "#111827",
          color: "#e5e7eb",
          marginBottom: 16,
        }}
      />

      <div
        style={{
          overflowX: "auto",
          borderRadius: 12,
          border: "1px solid #1f2937",
          marginBottom: 40,
        }}
      >
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
              <tr key={`${r.make}-${r.model}-${i}`} style={{ borderTop: "1px solid #1f2937" }}>
                <td style={td}>{r.make}</td>
                <td style={td}>{r.model}</td>
                <td style={td}>{r.tests.toLocaleString()}</td>
                <td style={td}>{r.fails.toLocaleString()}</td>
                <td style={td}>{r.fail_rate_pct.toFixed(2)}</td>
              </tr>
            ))}
            {filtered.length === 0 && (
              <tr>
                <td colSpan={5} style={{ padding: 16, textAlign: "center", opacity: 0.7 }}>
                  No results.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Fail rate vs year */}
      <h2 style={{ fontSize: 22, marginBottom: 16 }}>Fail Rate by Vehicle First-Use Year</h2>
      <div style={{ width: "100%", height: 300, marginBottom: 40 }}>
        <ResponsiveContainer>
          <LineChart data={yearRows}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="first_use_year" stroke="#e5e7eb" />
            <YAxis stroke="#e5e7eb" />
            <Tooltip />
            <Line type="monotone" dataKey="fail_rate_pct" stroke="#60a5fa" strokeWidth={2} dot={{ r: 2 }} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Top failure reasons */}
      <h2 style={{ fontSize: 22, margin: "0 0 16px" }}>Top Failure Reasons</h2>
      <div style={{ width: "100%", height: 400 }}>
        <ResponsiveContainer>
          <BarChart data={reasons}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis
              dataKey={(d: RowReason) => d.description || d.code}
              stroke="#e5e7eb"
              interval={0}
              angle={-30}
              textAnchor="end"
              height={120}
            />
            <YAxis stroke="#e5e7eb" />
            <Tooltip />
            <Bar dataKey="occurrences" fill="#f87171" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </main>
  );
}

const th: React.CSSProperties = { textAlign: "left", padding: "12px 14px", fontWeight: 600 };
const td: React.CSSProperties = { padding: "12px 14px" };
