const snapshotPath = "./status_snapshot.json";
const DEFAULT_PAGE = "overview";

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

function currentPageId() {
  const pageId = window.location.hash.replace("#", "").trim();
  return pageId || DEFAULT_PAGE;
}

function activatePage(pageId) {
  document.querySelectorAll(".page-panel").forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.page === pageId);
  });
  document.querySelectorAll("[data-page-link]").forEach((link) => {
    link.classList.toggle("active", link.dataset.pageLink === pageId);
  });
}

function renderWorkflowNav(pages) {
  const root = document.getElementById("workflow-nav");
  root.innerHTML = pages
    .map(
      (page) => `
      <a class="workflow-link" data-page-link="${page.id}" href="#${page.id}">
        <span class="workflow-link-title">${page.title}</span>
        <span class="workflow-link-summary">${page.summary}</span>
      </a>
    `
    )
    .join("");
  activatePage(currentPageId());
}

function renderWorkflowMap(pages) {
  const root = document.getElementById("workflow-map");
  root.innerHTML = pages
    .map(
      (page, index) => `
      <article class="workflow-card">
        <div class="workflow-step">Step ${index + 1}</div>
        <div class="workflow-title">${page.title}</div>
        <p class="workflow-summary">${page.summary}</p>
        <a class="workflow-jump" href="#${page.id}">Open page</a>
      </article>
    `
    )
    .join("");
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

function renderSourceCounts(metrics) {
  const sourceCounts = metrics.source_row_counts || [];
  return `
    <details class="details-card" open>
      <summary>Live source counts</summary>
      <div class="note-stack">
        ${sourceCounts
          .map(
            (row) => `<div class="note-item">${row.source_name}: ${Number(row.row_count || 0).toLocaleString()} rows in this checkpoint.</div>`
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
          ${columns.map((column) => `<td>${escapeHtml(row[column] ?? "")}</td>`).join("")}
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

function renderDatasetCollection({ targetId, payload, sampleAttr, samplePath }) {
  const root = document.getElementById(targetId);
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
          <button class="action-button" data-${sampleAttr}="${entry.key}">Fetch 10 random rows</button>
        </div>
        <p class="card-summary">${entry.simple_summary}</p>
        <details class="details-card">
          <summary>How this table is used</summary>
          <div class="note-stack">
            ${entry.notes.map((note) => `<div class="note-item">${note}</div>`).join("")}
          </div>
        </details>
        <details class="details-card">
          <summary>Column glossary</summary>
          ${renderGlossary(entry.columns)}
        </details>
        <div id="${sampleAttr}-sample-${entry.key}" class="query-result"></div>
      </article>
    `
    )
    .join("");

  root.querySelectorAll(`[data-${sampleAttr}]`).forEach((button) => {
    button.addEventListener("click", async () => {
      const key = button.getAttribute(`data-${sampleAttr}`);
      const target = `${sampleAttr}-sample-${key}`;
      setLoading(target, "Running live BigQuery sample query...");
      try {
        const samplePayload = await fetchJson(`${samplePath}/${key}/sample`);
        renderQueryResult(target, samplePayload);
      } catch (error) {
        setError(target, error);
      }
    });
  });
}

function renderStepCards(targetId, steps) {
  const root = document.getElementById(targetId);
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
      const target = `step-sample-${stepId}`;
      setLoading(target, "Running step sample query...");
      try {
        const payload = await fetchJson(`/api/registry/steps/${stepId}/sample`);
        renderQueryResult(target, payload);
      } catch (error) {
        setError(target, error);
      }
    });
  });
}

function renderHarmonizationScience(payload) {
  const root = document.getElementById("harmonization-science");
  root.innerHTML = `
    <article class="metric-item">
      <div class="metric-title">Scientific method for BRCA harmonization</div>
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
}

function renderPreGmeMeta(payload) {
  const root = document.getElementById("pre-gme-meta");
  root.innerHTML = `
    <article class="metric-item">
      <div class="metric-title">${payload.title}</div>
      <div class="metric-sub">${payload.table_ref}</div>
      <div class="metric-sub">row count: ${payload.row_count === null ? "not built yet" : Number(payload.row_count).toLocaleString()}</div>
      <div class="metric-sub">${payload.scope_note}</div>
      <div class="note-stack">
        ${payload.accuracy_notes.map((note) => `<div class="note-item">${note}</div>`).join("")}
      </div>
      <details class="details-card" open>
        <summary>Why this checkpoint exists</summary>
        <div class="note-stack">
          ${payload.scientific_notes.map((note) => `<div class="note-item">${note}</div>`).join("")}
        </div>
      </details>
      ${payload.scientific_metrics ? renderSourceCounts(payload.scientific_metrics) : ""}
    </article>
  `;

  document.getElementById("pre-gme-columns").innerHTML = payload.columns
    .map(
      (column) => `
      <article class="glossary-item">
        <div class="glossary-name">${column.name}</div>
        <div class="glossary-desc">${column.description}</div>
      </article>
    `
    )
    .join("");

  document.getElementById("pre-gme-build-sql").textContent = payload.build_sql;
  document.getElementById("pre-gme-metadata-preview").textContent = payload.export_metadata_preview.join("\n");
  document.getElementById("pre-gme-header-preview").innerHTML = payload.export_header_columns
    .map((column) => `<div class="header-chip">${escapeHtml(column)}</div>`)
    .join("");
  document.getElementById("pre-gme-download-link").setAttribute("href", payload.download_url);
}

function renderFinalRegistryMeta(payload) {
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
}

function renderGmeCard(payload) {
  const gmeOnly = {
    datasets: payload.datasets.filter((entry) => entry.key === "h_brca_gme_variants"),
  };
  renderDatasetCollection({
    targetId: "final-gme-card",
    payload: gmeOnly,
    sampleAttr: "harmonized-final-sample",
    samplePath: "/api/datasets",
  });
}

function wireGlobalButtons() {
  document.getElementById("pre-gme-sample-button").addEventListener("click", async () => {
    const target = "pre-gme-sample";
    setLoading(target, "Running live pre-GME sample query...");
    try {
      const payload = await fetchJson("/api/pre-gme/sample");
      renderQueryResult(target, payload);
    } catch (error) {
      setError(target, error);
    }
  });

  document.getElementById("registry-sample-button").addEventListener("click", async () => {
    const target = "registry-sample";
    setLoading(target, "Running live final-registry sample query...");
    try {
      const payload = await fetchJson("/api/registry/sample");
      renderQueryResult(target, payload);
    } catch (error) {
      setError(target, error);
    }
  });
}

async function main() {
  const generatedAtNode = document.getElementById("generated-at");

  try {
    const [snapshot, workflow, rawPayload, harmonizedPayload, preGmePayload, registryPayload] = await Promise.all([
      fetchJson(snapshotPath),
      fetchJson("/api/workflow"),
      fetchJson("/api/raw-datasets"),
      fetchJson("/api/datasets"),
      fetchJson("/api/pre-gme"),
      fetchJson("/api/registry"),
    ]);

    generatedAtNode.textContent = `Snapshot generated: ${snapshot.generated_at}`;
    renderWorkflowNav(workflow.pages);
    renderWorkflowMap(workflow.pages);
    renderStatusCards(snapshot.track_status_counts);
    renderTrackGrid(snapshot);
    renderRuntimeResults(snapshot.latest_t002_verification);
    renderDatasetCollection({
      targetId: "raw-dataset-explorer",
      payload: rawPayload,
      sampleAttr: "raw-sample",
      samplePath: "/api/raw-datasets",
    });
    renderHarmonizationScience(registryPayload);
    renderDatasetCollection({
      targetId: "harmonized-dataset-explorer",
      payload: harmonizedPayload,
      sampleAttr: "harmonized-sample",
      samplePath: "/api/datasets",
    });
    renderStepCards("harmonization-steps", workflow.harmonization_steps);
    renderPreGmeMeta(preGmePayload);
    renderGmeCard(harmonizedPayload);
    renderFinalRegistryMeta(registryPayload);
    renderStepCards("final-steps", workflow.final_steps);
  } catch (error) {
    generatedAtNode.textContent = `Failed to load dashboard: ${error.message}`;
  }

  wireGlobalButtons();
  activatePage(currentPageId());
}

window.addEventListener("hashchange", () => activatePage(currentPageId()));

main();
