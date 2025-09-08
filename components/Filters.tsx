"use client";
import { useEffect, useState } from "react";

export type Filters = {
  make?: string;
  model?: string;
  failure_category?: string;
  mileage_bucket?: string;
  month_year?: string;
};

export default function FiltersBar({
  options,
  onChange
}: {
  options: Record<keyof Filters, string[]>;
  onChange: (f: Filters) => void;
}) {
  const [filters, setFilters] = useState<Filters>({});

  useEffect(() => onChange(filters), [filters, onChange]);

  function Select({ label, field }: { label: string; field: keyof Filters }) {
    const opts = options[field] ?? [];
    return (
      <label className="flex flex-col gap-1">
        <span className="kpi-label">{label}</span>
        <select
          className="bg-bg border border-white/10 rounded-xl px-3 py-2"
          value={(filters[field] as string) || ""}
          onChange={(e) => setFilters((f) => ({ ...f, [field]: e.target.value || undefined }))}
        >
          <option value="">All</option>
          {opts.map((o) => (<option key={o} value={o}>{o}</option>))}
        </select>
      </label>
    );
  }

  return (
    <div className="card grid grid-cols-2 md:grid-cols-5 gap-4">
      <Select label="Make" field="make" />
      <Select label="Model" field="model" />
      <Select label="Category" field="failure_category" />
      <Select label="Mileage" field="mileage_bucket" />
      <Select label="Month" field="month_year" />
    </div>
  );
}
