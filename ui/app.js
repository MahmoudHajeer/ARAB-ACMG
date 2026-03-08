const snapshotPath = "./status_snapshot.json";

function toTitle(text) {
  return text.replaceAll("_", " ");
}

function escapeHtml(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function statusSymbol(label) {
  if (label === "done") return "Done";
  if (label === "in_progress") return "In Progress";
  return "Not Started";
}

function renderStatusCards(counts) {
  const root = document.getElementById("status-cards");
  const rows = [
    { key: "done", label: "Done Tracks" },
    { key: "in_progress", label: "In Progress" },
    { key: "not_started", label: "Not Started" },
  ];

  root.innerHTML = rows
    .map(
      (row) => `
      <article class="status-card">
        <div class="label">${row.label}</div>
        <div class="value">${counts[row.key] ?? 0}</div>
      </article>
    `
    )
    .join("");
}

function renderTrackGrid(snapshot) {
  const root = document.getElementById("track-grid");
  const items = snapshot.tracks.map((track) => {
    const progress = snapshot.plan_progress[track.track_id] || {};
    const pct = progress.progress_pct ?? 0;
    const done = progress.done_tasks ?? 0;
    const inProgress = progress.in_progress_tasks ?? 0;
    const todo = progress.todo_tasks ?? 0;

    return `
      <article class="track-row">
        <div class="track-head">
          <div class="track-title">${track.track_id} • ${track.name}</div>
          <span class="track-status ${track.status_label}">${statusSymbol(track.status_label)}</span>
        </div>
        <p class="track-desc">${track.description}</p>
        <div class="progress-wrap"><div class="progress-bar" style="width: ${pct}%"></div></div>
        <div class="progress-meta">
          <span>${pct.toFixed(1)}% weighted progress</span>
          <span>done ${done} / active ${inProgress} / todo ${todo}</span>
        </div>
      </article>
    `;
  });
  root.innerHTML = items.join("");
}

function renderRuntimeResults(results) {
  const root = document.getElementById("runtime-results");
  if (!results.length) {
    root.innerHTML = `<div class="runtime-item"><span>No runtime entries found yet.</span></div>`;
    return;
  }
  root.innerHTML = results
    .map(
      (item) => `
      <article class="runtime-item">
        <code class="runtime-cmd">${item.command}</code>
        <span class="pill ${item.status}">${toTitle(item.status)}</span>
      </article>
    `
    )
    .join("");
}

function renderBigQueryMetrics(metrics) {
  const root = document.getElementById("bq-metrics");
  if (metrics.error) {
    root.innerHTML = `<article class="metric-item"><div class="metric-title">BigQuery Error</div><div class="metric-sub">${metrics.error}</div></article>`;
    return;
  }

  const maxRows = Math.max(...metrics.tables.map((row) => row.rows), 1);
  root.innerHTML = metrics.tables
    .map((table) => {
      const width = (table.rows / maxRows) * 100;
      const fields = table.fields.join(", ");
      const statusNote = table.status === "missing" ? "status: missing (not loaded yet)" : "status: present";
      return `
        <article class="metric-item">
          <div class="metric-title">${table.table}</div>
          <div class="metric-sub">rows: ${table.rows.toLocaleString()}</div>
          <div class="metric-sub">${statusNote}</div>
          <div class="metric-sub">fields: ${fields || "n/a"}</div>
          <div class="bar-inline"><span style="width: ${width}%"></span></div>
        </article>
      `;
    })
    .join("");
}

function renderGcsMetrics(metrics) {
  const root = document.getElementById("gcs-metrics");
  if (metrics.error) {
    root.innerHTML = `<article class="metric-item"><div class="metric-title">GCS Error</div><div class="metric-sub">${metrics.error}</div></article>`;
    return;
  }

  const maxCount = Math.max(...metrics.prefixes.map((row) => row.count), 1);
  root.innerHTML = metrics.prefixes
    .map((entry) => {
      const width = (entry.count / maxCount) * 100;
      const samples = entry.sample_paths.length
        ? entry.sample_paths.map((path) => `<div>${path}</div>`).join("")
        : "<div>(no objects)</div>";

      return `
        <article class="metric-item">
          <div class="metric-title">${entry.prefix}</div>
          <div class="metric-sub">objects: ${entry.count}</div>
          <div class="bar-inline"><span style="width: ${width}%"></span></div>
          <div class="metric-sub">${samples}</div>
        </article>
      `;
    })
    .join("");
}

function renderBigQuerySamples(samples) {
  const root = document.getElementById("bq-samples");
  if (samples.error) {
    root.innerHTML = `<article class="metric-item"><div class="metric-title">Sample Error</div><div class="metric-sub">${samples.error}</div></article>`;
    return;
  }

  root.innerHTML = samples.tables
    .map((entry) => {
      if (entry.status === "missing") {
        return `
          <article class="sample-card">
            <div class="sample-title">${entry.table}</div>
            <div class="metric-sub">table missing</div>
          </article>
        `;
      }

      const head = entry.columns.map((column) => `<th>${column}</th>`).join("");
      const body = entry.rows
        .map(
          (row) => `
            <tr>
              ${
                entry.columns
                  .map((column) => {
                    const value = row[column] || "";
                    return `<td title="${escapeHtml(value)}">${escapeHtml(value)}</td>`;
                  })
                  .join("")
              }
            </tr>
          `
        )
        .join("");

      return `
        <article class="sample-card">
          <div class="sample-title">${entry.table}</div>
          <div class="sample-table-wrap">
            <table class="sample-table">
              <thead><tr>${head}</tr></thead>
              <tbody>${body}</tbody>
            </table>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderRoadmap(phases) {
  const root = document.getElementById("roadmap-phases");
  root.innerHTML = phases
    .map(
      (phase) => `
      <article class="phase-item">
        <div class="phase-title">${phase.title}</div>
        <ul class="phase-bullets">
          ${phase.bullets.map((bullet) => `<li>${bullet}</li>`).join("")}
        </ul>
      </article>
    `
    )
    .join("");
}

async function main() {
  const generatedAtNode = document.getElementById("generated-at");
  try {
    const response = await fetch(snapshotPath, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Snapshot not found (${response.status})`);
    }
    const snapshot = await response.json();
    generatedAtNode.textContent = `Snapshot generated: ${snapshot.generated_at}`;
    renderStatusCards(snapshot.track_status_counts);
    renderTrackGrid(snapshot);
    renderRuntimeResults(snapshot.latest_t002_verification);
    renderBigQueryMetrics(snapshot.bigquery_metrics);
    renderBigQuerySamples(snapshot.bigquery_samples);
    renderGcsMetrics(snapshot.gcs_metrics);
    renderRoadmap(snapshot.data_collection_roadmap);
  } catch (error) {
    generatedAtNode.textContent = `Failed to load snapshot: ${error.message}`;
  }
}

main();
