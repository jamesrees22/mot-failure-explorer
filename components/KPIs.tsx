export default function KPIs({
  totalFailures,
  topCategory,
  topMake,
  period
}: {
  totalFailures: number;
  topCategory: string;
  topMake: string;
  period: string;
}) {
  return (
    <div className="grid md:grid-cols-4 gap-6">
      <div className="card bg-gray-800 border-red-900">
        <div className="kpi-label text-red-400">Total Failures</div>
        <div className="kpi-value">{totalFailures.toLocaleString()}</div>
      </div>
      <div className="card bg-gray-800 border-red-900">
        <div className="kpi-label text-red-400">Top Category</div>
        <div className="kpi-value">{topCategory || "-"}</div>
      </div>
      <div className="card bg-gray-800 border-red-900">
        <div className="kpi-label text-red-400">Top Make</div>
        <div className="kpi-value">{topMake || "-"}</div>
      </div>
      <div className="card bg-gray-800 border-red-900">
        <div className="kpi-label text-red-400">Period</div>
        <div className="kpi-value">{period}</div>
      </div>
    </div>
  );
}