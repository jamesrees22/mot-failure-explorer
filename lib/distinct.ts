import { supabase } from "./supabase";

const PAGE = 10000;

export async function distinctValues(
  column: string,
  where?: Record<string, string | undefined>
): Promise<string[]> {
  const values = new Set<string>();
  let from = 0;
  while (true) {
    let q = supabase
      .from("aggregated_failures_2024")
      .select(column, { count: "exact" })
      .range(from, from + PAGE - 1);
    if (where) {
      Object.entries(where).forEach(([k, v]) => {
        if (v) q = q.eq(k, v);
      });
    }
    const { data, error } = await q;
    if (error) throw error;
    (data ?? []).forEach((r: any) => {
      const v = r[column];
      if (typeof v === "string" && v.length) values.add(v);
    });
    if (!data || data.length < PAGE) break;
    from += PAGE;
  }
  return Array.from(values).sort((a, b) => a.localeCompare(b));
}