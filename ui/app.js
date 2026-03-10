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

async function fetchJson(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`${path} -> ${response.status}`);
  }
  return response.json();
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
        <code class="runtime-cmd">${escapeHtml(item.command)}</code>
        <span class="pill ${item.status}">${toTitle(item.status)}</span>
      </article>
    `
    )
    .join("");
}

function renderGlossary(columns) {
  return `
    <div class="glossary-grid">
      ${columns
        .map(
          (column) => `
          <article class="glossary-item">
            <div class="glossary-name">${column.name}</div>
            <div class="glossary-desc">${column.description}</div>
          </article>
        `
        )
        .join("")}
    </div>
  `;
}

function renderScientificMetrics(metrics) {
  const windows = metrics.gene_windows || [];
  const clinvarAudit = metrics.clinvar_gene_audit || [];
  const sourceCounts = metrics.source_row_counts || [];

  return `
    <details class="details-card" open>
      <summary>Live scientific evidence</summary>
      <div class="note-stack">
        ${windows
          .map(
            (row) => `<div class="note-item">${row.gene_symbol}: ${row.chrom38}:${row.start_pos38}-${row.end_pos38} | source ${row.coordinate_source} | url ${row.coordinate_source_url} | accessed ${row.accessed_at}</div>`
          )
          .join("")}
        ${clinvarAudit
          .map(
            (row) => `<div class="note-item">ClinVar ${row.gene_symbol}: ${Number(row.gene_info_mismatch_rows || 0).toLocaleString()} gene-label mismatches inside ${Number(row.clinvar_window_rows || 0).toLocaleString()} harmonized window rows, and ${Number(row.gene_label_outside_window_rows || 0).toLocaleString()} BRCA-labeled rows outside the strict Ensembl window in staging.</div>`
          )
          .join("")}
        ${sourceCounts
          .map(
            (row) => `<div class="note-item">${row.source_name}: ${Number(row.row_count || 0).toLocaleString()} harmonized BRCA rows.</div>`
          )
          .join("")}
      </div>
    </details>
  `;
}

function renderQueryResult(targetId, payload) {
  const root = document.getElementById(targetId);
  const columns = payload.columns || [];
  const rows = payload.rows || [];

  if (!columns.length) {
    root.innerHTML = `<div class="empty-state">No rows returned.</div>`;
    return;
  }

  const head = columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("");
  const body = rows
    .map(
      (row) => `
        <tr>
          ${columns
            .map((column) => `<td>${escapeHtml(row[column] ?? "")}</td>`)
            .join("")}
        </tr>
      `
    )
    .join("");

  root.innerHTML = `
    <details class="details-card">
      <summary>Query used</summary>
      <div class="query-meta">
        <pre class="sql-box">${escapeHtml(payload.query_sql)}</pre>
      </div>
    </details>
    <div class="sample-table-wrap">
      <table class="sample-table live-table">
        <thead><tr>${head}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function setLoading(targetId, label) {
  document.getElementById(targetId).innerHTML = `<div class="loading-state">${label}</div>`;
}

function setError(targetId, error) {
  document.getElementById(targetId).innerHTML = `<div class="error-state">${escapeHtml(error.message || error)}</div>`;
}

function renderDatasetExplorer(payload) {
  const root = document.getElementById("dataset-explorer");
  root.innerHTML = payload.datasets
    .map(
      (entry) => `
      <article class="explorer-card">
        <div class="card-head">
          <div>
            <div class="sample-title">${entry.title}</div>
            <div class="metric-sub">${entry.table_ref}</div>
            <div class="metric-sub">row count: ${(entry.row_count || 0).toLocaleString()}</div>
          </div>
          <button class="action-button" data-dataset-sample="${entry.key}">Fetch 10 random rows</button>
        </div>
        <p class="card-summary">${entry.simple_summary}</p>
        <details class="details-card">
          <summary>How this source was extracted</summary>
          <div class="note-stack">
            ${entry.notes.map((note) => `<div class="note-item">${note}</div>`).join("")}
          </div>
        </details>
        <details class="details-card">
          <summary>Column glossary</summary>
          ${renderGlossary(entry.columns)}
        </details>
        <div id="dataset-sample-${entry.key}" class="query-result"></div>
      </article>
    `
    )
    .join("");

  root.querySelectorAll("[data-dataset-sample]").forEach((button) => {
    button.addEventListener("click", async () => {
      const key = button.dataset.datasetSample;
      const targetId = `dataset-sample-${key}`;
      setLoading(targetId, "Running live BigQuery sample query...");
      try {
        const payload = await fetchJson(`/api/datasets/${key}/sample`);
        renderQueryResult(targetId, payload);
      } catch (error) {
        setError(targetId, error);
      }
    });
  });
}

function renderRegistrySteps(steps) {
  const root = document.getElementById("registry-steps");
  root.innerHTML = steps
    .map(
      (step) => `
      <article class="explorer-card">
        <div class="card-head">
          <div class="sample-title">${step.title}</div>
          <button class="action-button secondary" data-step-sample="${step.id}">Run 10-row sample</button>
        </div>
        <p class="card-summary">${step.simple}</p>
        <details class="details-card">
          <summary>Technical note</summary>
          <div class="metric-sub">${step.technical}</div>
        </details>
        <div id="step-sample-${step.id}" class="query-result"></div>
      </article>
    `
    )
    .join("");

  root.querySelectorAll("[data-step-sample]").forEach((button) => {
    button.addEventListener("click", async () => {
      const stepId = button.dataset.stepSample;
      const targetId = `step-sample-${stepId}`;
      setLoading(targetId, "Running step sample query...");
      try {
        const payload = await fetchJson(`/api/registry/steps/${stepId}/sample`);
        renderQueryResult(targetId, payload);
      } catch (error) {
        setError(targetId, error);
      }
    });
  });
}

function renderRegistryMeta(payload) {
  const metaRoot = document.getElementById("registry-meta");
  metaRoot.innerHTML = `
    <article class="metric-item">
      <div class="metric-title">${payload.title}</div>
      <div class="metric-sub">${payload.table_ref}</div>
      <div class="metric-sub">row count: ${payload.row_count === null ? "not built yet" : Number(payload.row_count).toLocaleString()}</div>
      <div class="metric-sub">${payload.scope_note}</div>
      <div class="note-stack">
        ${payload.accuracy_notes.map((note) => `<div class="note-item">${note}</div>`).join("")}
      </div>
      <details class="details-card" open>
        <summary>Scientific explanation</summary>
        <div class="note-stack">
          ${payload.scientific_notes.map((note) => `<div class="note-item">${note}</div>`).join("")}
        </div>
      </details>
      ${payload.scientific_metrics ? renderScientificMetrics(payload.scientific_metrics) : ""}
    </article>
  `;

  document.getElementById("registry-columns").innerHTML = payload.columns
    .map(
      (column) => `
      <article class="glossary-item">
        <div class="glossary-name">${column.name}</div>
        <div class="glossary-desc">${column.description}</div>
      </article>
    `
    )
    .join("");

  document.getElementById("registry-build-sql").textContent = payload.build_sql;
  renderRegistrySteps(payload.steps);
}

async function main() {
  const generatedAtNode = document.getElementById("generated-at");

  try {
    const [snapshot, datasetsPayload, registryPayload] = await Promise.all([
      fetchJson(snapshotPath),
      fetchJson("/api/datasets"),
      fetchJson("/api/registry"),
    ]);

    generatedAtNode.textContent = `Snapshot generated: ${snapshot.generated_at}`;
    renderStatusCards(snapshot.track_status_counts);
    renderTrackGrid(snapshot);
    renderRuntimeResults(snapshot.latest_t002_verification);
    renderDatasetExplorer(datasetsPayload);
    renderRegistryMeta(registryPayload);
  } catch (error) {
    generatedAtNode.textContent = `Failed to load dashboard: ${error.message}`;
  }

  document.getElementById("registry-sample-button").addEventListener("click", async () => {
    const targetId = "registry-sample";
    setLoading(targetId, "Running live registry sample query...");
    try {
      const payload = await fetchJson("/api/registry/sample");
      renderQueryResult(targetId, payload);
    } catch (error) {
      setError(targetId, error);
    }
  });
}

main();
