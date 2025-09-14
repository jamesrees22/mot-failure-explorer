"use client";
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, LineChart, Line, CartesianGrid } from "recharts";

export function CategoryBar({ data }: { data: { name: string; value: number }[] }) {
  return (
    <div className="card h-[340px]">
      <div className="kpi-label mb-2">Failures by Category</div>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e0e6ed" opacity={0.3} />
          <XAxis dataKey="name" tick={{ fontSize: 12, fill: "#6b7280" }} />
          <YAxis tick={{ fontSize: 12, fill: "#6b7280" }} />
          <Tooltip contentStyle={{ backgroundColor: "#ffffff", border: "1px solid #e0e6ed", color: "#374151" }} />
          <Bar dataKey="value" fill="#4a90e2" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

export function TrendLine({ data }: { data: { name: string; value: number }[] }) {
  return (
    <div className="card h-[340px]">
      <div className="kpi-label mb-2">Monthly Trend</div>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e0e6ed" opacity={0.3} />
          <XAxis dataKey="name" tick={{ fontSize: 12, fill: "#6b7280" }} />
          <YAxis tick={{ fontSize: 12, fill: "#6b7280" }} />
          <Tooltip contentStyle={{ backgroundColor: "#ffffff", border: "1px solid #e0e6ed", color: "#374151" }} />
          <Line type="monotone" dataKey="value" stroke="#4a90e2" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}