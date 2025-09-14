"use client";
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, LineChart, Line, CartesianGrid } from "recharts";

export function CategoryBar({ data }: { data: { name: string; value: number }[] }) {
  return (
    <div className="card h-[340px] bg-gray-800 border-red-900">
      <div className="kpi-label mb-2 text-red-400">Failures by Category</div>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#4b5563" opacity={0.1} />
          <XAxis dataKey="name" tick={{ fontSize: 12, fill: "#d1d5db" }} />
          <YAxis tick={{ fontSize: 12, fill: "#d1d5db" }} />
          <Tooltip contentStyle={{ backgroundColor: "#1f2937", border: "none", color: "#d1d5db" }} />
          <Bar dataKey="value" fill="#ef4444" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

export function TrendLine({ data }: { data: { name: string; value: number }[] }) {
  return (
    <div className="card h-[340px] bg-gray-800 border-red-900">
      <div className="kpi-label mb-2 text-red-400">Monthly Trend</div>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#4b5563" opacity={0.1} />
          <XAxis dataKey="name" tick={{ fontSize: 12, fill: "#d1d5db" }} />
          <YAxis tick={{ fontSize: 12, fill: "#d1d5db" }} />
          <Tooltip contentStyle={{ backgroundColor: "#1f2937", border: "none", color: "#d1d5db" }} />
          <Line type="monotone" dataKey="value" stroke="#ef4444" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}