import { supabase } from "@/lib/supabase";
import { distinctValues } from "@/lib/distinct";
import FiltersBar, { Filters } from "@/components/Filters";
import KPIs from "@/components/KPIs";
import { CategoryBar, TrendLine } from "@/components/Charts";

async function fetchOptions(filters?: Filters) {
  const [{ data: makes }, { data: models }, { data: cats }, { data: miles }, { data: months }] = await Promise.all([
    supabase.from("mv_mot24_makes").select("make").order("failures", { ascending: false }),
    filters?.make
      ? supabase.from("mv_mot24_models").select("model").eq("make", filters.make)
      : supabase.from("mv_mot24_models").select("model").limit(2000),
    supabase.from("mv_mot24_categories").select("failure_category"),
    supabase.from("mv_mot24_mileage").select("mileage_bucket"),
    supabase.from("mv_mot24_months").select("month_year")
  ]);

  return {
    make: Array.from(new Set((makes ?? []).map((r: any) => r.make))).filter(Boolean),
    model: Array.from(new Set((models ?? []).map((r: any) => r.model))).filter(Boolean),
    failure_category: Array.from(new Set((cats ?? []).map((r: any) => r.failure_category))).filter(Boolean),
    mileage_bucket: Array.from(new Set((miles ?? []).map((r: any) => r.mileage_bucket))).filter(Boolean),
    month_year: Array.from(new Set((months ?? []).map((r: any) => r.month_year))).filter(Boolean)
  };
}

async function fetchData(filters: Filters) {
  let q = supabase.from("aggregated_failures_2024").select("*");
  (Object.keys(filters) as (keyof Filters)[]).forEach((k) => {
    const v = filters[k];
    if (v) q = q.eq(k, v);
  });
  const { data, error } = await q;
  if (error) throw error;

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
  const filters: Filters = {
    make: searchParams.make,
    model: searchParams.model,
    failure_category: searchParams.failure_category,
    mileage_bucket: searchParams.mileage_bucket,
    month_year: searchParams.month_year
  };

  const [options, { totalFailures, catArr, monthArr, topMake }] = await Promise.all([
    fetchOptions(filters),
    fetchData(filters)
  ]);

  const topCategory = catArr[0]?.name ?? "";
  const period = filters.month_year ?? "2024";

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="font-mono text-soft">TACTICAL COMMAND / <span className="text-white">OVERVIEW</span></div>
        <div className="text-xs font-mono text-soft">LAST UPDATE: 2024-12-31 23:59 UTC</div>
      </div>

      {/* client component manages URL updates */}
      <FiltersBar options={options} initial={filters} />

      <KPIs totalFailures={totalFailures} topCategory={topCategory} topMake={topMake} period={period} />

      <div className="grid md:grid-cols-2 gap-4">
        <CategoryBar data={catArr.slice(0, 12)} />
        <TrendLine data={monthArr} />
      </div>
    </div>
  );
}
