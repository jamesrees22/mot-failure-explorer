export const revalidate = 0; // disable caching for this page
import { supabase } from "@/lib/supabase";
import { distinctValues } from "@/lib/distinct";
import FiltersBar, { Filters } from "@/components/Filters";
import KPIs from "@/components/KPIs";
import { CategoryBar, TrendLine } from "@/components/Charts";

function uniqPreserve<T>(arr: T[]) {
  const seen = new Set<string>();
  const out: T[] = [];
  for (const v of arr) {
    const key = String(v ?? "");
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(v);
  }
  return out;
}

async function fetchOptions(filters?: Filters) {
  const [{ data: makes }, { data: models }, { data: cats }, { data: miles }, { data: months }] = await Promise.all([
    // A→Z explicitly
    supabase.from("mv_mot24_makes").select("make").order("make", { ascending: true }),
    filters?.make
      ? supabase
          .from("mv_mot24_models")
          .select("model")
          .eq("make", filters.make)
          .order("model", { ascending: true })
      : supabase
          .from("mv_mot24_models")
          .select("model")
          .order("model", { ascending: true })
          .limit(5000),
    supabase.from("mv_mot24_categories").select("failure_category").order("failure_category", { ascending: true }),
    supabase.from("mv_mot24_mileage").select("mileage_bucket").order("mileage_bucket", { ascending: true }),
    supabase.from("mv_mot24_months").select("month_year").order("month_year", { ascending: true })
  ]);

  return {
    make: uniqPreserve((makes ?? []).map((r: any) => r.make)),
    model: uniqPreserve((models ?? []).map((r: any) => r.model)),
    failure_category: uniqPreserve((cats ?? []).map((r: any) => r.failure_category)),
    mileage_bucket: uniqPreserve((miles ?? []).map((r: any) => r.mileage_bucket)),
    month_year: uniqPreserve((months ?? []).map((r: any) => r.month_year))
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
