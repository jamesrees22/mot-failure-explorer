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
      <div className="card p-4">
        <div className="kpi-label">Total Failures</div>
        <div className="kpi-value">{totalFailures.toLocaleString()}</div>
      </div>
      <div className="card p-4">
        <div className="kpi-label">Top Category</div>
        <div className="kpi-value">{topCategory || "-"}</div>
      </div>
      <div className="card p-4">
        <div className="kpi-label">Top Make</div>
        <div className="kpi-value">{topMake || "-"}</div>
      </div>
      <div className="card p-4">
        <div className="kpi-label">Period</div>
        <div className="kpi-value">{period}</div>
      </div>
    </div>
  );
}