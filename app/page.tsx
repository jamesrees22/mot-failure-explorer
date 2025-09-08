import { supabase } from "@/lib/supabase";
import FiltersBar, { Filters } from "@/components/Filters";
import KPIs from "@/components/KPIs";
import { CategoryBar, TrendLine } from "@/components/Charts";

async function fetchOptions() {
  const fields: (keyof Filters)[] = ["make", "model", "failure_category", "mileage_bucket", "month_year"];
  const options: Record<string, string[]> = {};
  for (const f of fields) {
    const { data } = await supabase.from("aggregated_failures_2024").select(`${f}`).order(f, { ascending: true });
    options[f] = Array.from(new Set((data ?? []).map((r: any) => r[f]))).filter(Boolean);
  }
  return options as Record<keyof Filters, string[]>;
}

async function fetchData(filters: Filters) {
  let q = supabase.from("aggregated_failures_2024").select("*");
  (Object.keys(filters) as (keyof Filters)[]).forEach((k) => {
    const v = filters[k];
    if (v) q = q.eq(k, v);
  });
  const { data } = await q.limit(200000);

  const totalFailures = (data ?? []).reduce((a: number, r: any) => a + r.failure_count, 0);

  const byCat: Record<string, number> = {};
  const byMonth: Record<string, number> = {};
  const byMake: Record<string, number> = {};
  (data ?? []).forEach((r: any) => {
    byCat[r.failure_category] = (byCat[r.failure_category] || 0) + r.failure_count;
    byMonth[r.month_year] = (byMonth[r.month_year] || 0) + r.failure_count;
    byMake[r.make] = (byMake[r.make] || 0) + r.failure_count;
  });

  const catArr = Object.entries(byCat).map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value);
  const monthArr = Object.entries(byMonth).sort(([a], [b]) => (a < b ? -1 : 1)).map(([name, value]) => ({ name, value }));
  const topMake = Object.entries(byMake).sort((a, b) => b[1] - a[1])[0]?.[0] ?? "";

  return { totalFailures, catArr, monthArr, topMake };
}

export default async function Page({ searchParams }: { searchParams: Filters }) {
  const options = await fetchOptions();
  const filters: Filters = {
    make: searchParams.make,
    model: searchParams.model,
    failure_category: searchParams.failure_category,
    mileage_bucket: searchParams.mileage_bucket,
    month_year: searchParams.month_year
  };

  const { totalFailures, catArr, monthArr, topMake } = await fetchData(filters);
  const topCategory = catArr[0]?.name ?? "";
  const period = filters.month_year ?? "2024";

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="font-mono text-soft">TACTICAL COMMAND / <span className="text-white">OVERVIEW</span></div>
        <div className="text-xs font-mono text-soft">LAST UPDATE: 2024-12-31 23:59 UTC</div>
      </div>

      {/* pass only serializable props */}
      <FiltersBar options={options} initial={filters} />

      <KPIs totalFailures={totalFailures} topCategory={topCategory} topMake={topMake} period={period} />

      <div className="grid md:grid-cols-2 gap-4">
        <CategoryBar data={catArr.slice(0, 12)} />
        <TrendLine data={monthArr} />
      </div>
    </div>
  );
}
