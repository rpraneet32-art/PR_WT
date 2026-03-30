import { useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip } from "recharts";

export default function App() {
  const [query, setQuery] = useState("COUNT");
  const [accuracy, setAccuracy] = useState(0.9);
  const [result, setResult] = useState(null);

  const runQuery = () => {
    const acc = parseFloat(accuracy);

    setResult({
      exact: 100000,
      approx: Math.floor(100000 * acc),
      exact_time: 1.2,
      approx_time: 0.3 * acc,
    });
  };

  const chartData = result
    ? [
        { name: "Exact", time: result.exact_time },
        { name: "Approx", time: result.approx_time },
      ]
    : [];

  return (
    <div
      style={{
        padding: "30px",
        fontFamily: "Arial",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        background: "#f5f5f5",
        minHeight: "100vh",
      }}
    >
      <h1>Approx Query Engine 🚀</h1>

      <p style={{ color: "#555" }}>
        Trade accuracy for speed using approximate algorithms ⚡
      </p>

      {/* Query Selector */}
      <select
        onChange={(e) => setQuery(e.target.value)}
        style={{ padding: "8px", marginTop: "10px" }}
      >
        <option>COUNT</option>
        <option>SUM</option>
        <option>AVG</option>
      </select>

      {/* Accuracy Slider */}
      <div style={{ marginTop: "20px", width: "300px" }}>
        <label>Accuracy: {accuracy}</label>
        <input
          type="range"
          min="0.8"
          max="0.99"
          step="0.01"
          value={accuracy}
          onChange={(e) => setAccuracy(parseFloat(e.target.value))}
          style={{ width: "100%" }}
        />
      </div>

      {/* Button */}
      <button
        onClick={runQuery}
        style={{
          marginTop: "20px",
          padding: "10px 20px",
          background: "#4CAF50",
          color: "white",
          border: "none",
          borderRadius: "5px",
          cursor: "pointer",
        }}
      >
        Run Query
      </button>

      {/* Results Section */}
      {result && (
        <div
          style={{
            marginTop: "30px",
            padding: "20px",
            background: "#fff",
            borderRadius: "10px",
            boxShadow: "0 0 10px rgba(0,0,0,0.1)",
            width: "400px",
          }}
        >
          <h2>Results</h2>

          <p>Exact: {result.exact}</p>
          <p>Approx: {result.approx}</p>
          <p>Exact Time: {result.exact_time}s</p>
          <p>Approx Time: {result.approx_time}s</p>

          {/* Speedup */}
          <p>
            Speedup:{" "}
            {result.approx_time > 0
              ? (result.exact_time / result.approx_time).toFixed(2)
              : "∞"}
            x faster 🚀
          </p>

          {/* Error */}
          <p>
            Error:{" "}
            {result.exact > 0
              ? (
                  (Math.abs(result.exact - result.approx) /
                    result.exact) *
                  100
                ).toFixed(2)
              : 0}
            %
          </p>

          {/* Insight Line */}
          <p style={{ color: "green", fontWeight: "bold" }}>
            ⚡ Approximate queries significantly reduce computation time while
            maintaining acceptable accuracy.
          </p>

          {/* Chart */}
          <h3>Performance Chart</h3>
          <BarChart width={350} height={250} data={chartData}>
            <XAxis dataKey="name" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="time" />
          </BarChart>
        </div>
      )}
    </div>
  );
}