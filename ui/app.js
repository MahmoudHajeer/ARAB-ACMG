const DEFAULT_PAGE = "overview";
const EXTRA_WORKFLOW_PAGES = [
  {
    id: "access",
    title: "Controlled Access",
    summary: "Official access paths for large Arab datasets that still require provider approval.",
  },
];
const KNOWN_PAGE_IDS = new Set(["overview", "raw", "harmonization", "access", "pre-gme", "final"]);
const resourceCache = new Map();
const inflightRequests = new Map();
const renderedPages = new Set();
let workflowPayload = null;
let overviewPayload = null;
let globalButtonsWired = false;

// [AI-Agent: Codex]: Section 1 / Core text helpers - keep all UI rendering escaped and deterministic because the page is built from frozen JSON artifacts.
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

async function fetchResource(key, path) {
  if (resourceCache.has(key)) {
    return resourceCache.get(key);
  }

  if (inflightRequests.has(key)) {
    return inflightRequests.get(key);
  }

  const request = fetchJson(path)
    .then((payload) => {
      resourceCache.set(key, payload);
      return payload;
    })
    .finally(() => {
      inflightRequests.delete(key);
    });

  inflightRequests.set(key, request);
  return request;
}

function workflowPages() {
  const basePages = workflowPayload?.pages || [];
  const seen = new Set(basePages.map((page) => page.id));
  return [...basePages, ...EXTRA_WORKFLOW_PAGES.filter((page) => !seen.has(page.id))];
}

function currentPageId() {
  const pageId = window.location.hash.replace("#", "").trim();
  if (!pageId || !KNOWN_PAGE_IDS.has(pageId)) {
    return DEFAULT_PAGE;
  }
  return pageId;
}

function activatePage(pageId) {
  document.querySelectorAll(".page-panel").forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.page === pageId);
  });
  document.querySelectorAll("[data-page-link]").forEach((link) => {
    link.classList.toggle("active", link.dataset.pageLink === pageId);
  });
}

function renderNoteStack(items) {
  return `
    <div class="note-stack">
      ${items.map((item) => `<div class="note-item">${escapeHtml(item)}</div>`).join("")}
    </div>
  `;
}

function renderTraceSummary(trace) {
  if (!trace) {
    return "";
  }

  const rows = [
    ["Input", trace.input_surface],
    ["Operation", trace.operation],
    ["Count", trace.count_basis],
    ["Display", trace.display_basis],
  ].filter(([, value]) => Boolean(value));

  return `
    <details class="details-card trace-card">
      <summary>Trace</summary>
      <div class="trace-grid">
        ${rows
          .map(
            ([label, value]) => `
            <article class="trace-item">
              <div class="trace-label">${escapeHtml(label)}</div>
              <div class="trace-value">${escapeHtml(value)}</div>
            </article>
          `
          )
          .join("")}
      </div>
    </details>
  `;
}

// [AI-Agent: Codex]: Section 2 / Navigation and overview rendering - the supervisor should see the current stage first, then drill into evidence only when needed.
function renderWorkflowNav(pages) {
  const root = document.getElementById("workflow-nav");
  root.innerHTML = pages
    .map(
      (page) => `
      <a class="workflow-link" data-page-link="${page.id}" href="#${page.id}">
        <span class="workflow-link-step">${escapeHtml(page.id)}</span>
        <span class="workflow-link-title">${escapeHtml(page.title)}</span>
        <span class="workflow-link-summary">${escapeHtml(page.summary)}</span>
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
        <div class="workflow-title">${escapeHtml(page.title)}</div>
        <p class="workflow-summary">${escapeHtml(page.summary)}</p>
        <a class="workflow-jump" href="#${page.id}">Open page</a>
      </article>
    `
    )
    .join("");
}

function renderLinkList(links) {
  return `
    <div class="link-list">
      ${links
        .map(
          (link) => `
          <a class="action-button secondary-link compact-link" href="${escapeHtml(link.url)}" target="_blank" rel="noreferrer">
            ${escapeHtml(link.label)}
          </a>
        `
        )
        .join("")}
    </div>
  `;
}

// [AI-Agent: Codex]: Review Stage A - explain the harmonization workflow as explicit scientific gates so the supervisor can audit each decision boundary.
function renderWorkflowCategories(categories) {
  const root = document.getElementById("workflow-categories");
  root.innerHTML = categories
    .map(
      (category) => `
      <article class="workflow-card scientific-card">
        <div class="workflow-step">${escapeHtml(category.id)}</div>
        <div class="workflow-title">${escapeHtml(category.title)}</div>
        <p class="workflow-summary">${escapeHtml(category.purpose)}</p>
        <details class="details-card">
          <summary>Evidence used</summary>
          ${renderNoteStack(category.evidence_types.map((item) => `Evidence: ${item}`))}
        </details>
        <details class="details-card">
          <summary>Output of this stage</summary>
          <div class="metric-sub">${escapeHtml(category.output)}</div>
        </details>
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
          <div>
            <div class="track-title">${escapeHtml(track.track_id)} • ${escapeHtml(track.name)}</div>
            <p class="track-desc">${escapeHtml(track.description)}</p>
          </div>
          <span class="track-status ${track.status_label}">${statusSymbol(track.status_label)}</span>
        </div>
        <div class="progress-wrap" aria-hidden="true"><div class="progress-bar" style="width: ${pct}%"></div></div>
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
    root.innerHTML = `<div class="runtime-item"><span>No verification entries were found in the latest T002 handoff log.</span></div>`;
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
          <article class="glossary-item ${column.kind || "required"}">
            <div class="glossary-name">
              ${escapeHtml(column.name)}
              <span class="column-kind ${column.kind || "required"}">${escapeHtml((column.kind || "required").toUpperCase())}</span>
            </div>
            <div class="glossary-desc">${escapeHtml(column.description)}</div>
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
  const frozenAt = metrics.frozen_at || "frozen snapshot";

  return `
    <details class="details-card">
      <summary>Frozen scientific evidence (${escapeHtml(frozenAt)})</summary>
      <div class="note-stack">
        ${windows
          .map(
            (row) =>
              `<div class="note-item">${escapeHtml(row.gene_symbol)}: ${escapeHtml(row.chrom38)}:${escapeHtml(row.start_pos38)}-${escapeHtml(row.end_pos38)} | source ${escapeHtml(row.coordinate_source)} | url ${escapeHtml(row.coordinate_source_url)} | accessed ${escapeHtml(row.accessed_at)}</div>`
          )
          .join("")}
        ${clinvarAudit
          .map(
            (row) =>
              `<div class="note-item">ClinVar ${escapeHtml(row.gene_symbol)}: ${Number(row.gene_info_mismatch_rows || 0).toLocaleString()} gene-label mismatches inside ${Number(row.clinvar_window_rows || 0).toLocaleString()} harmonized window rows, and ${Number(row.gene_label_outside_window_rows || 0).toLocaleString()} BRCA-labeled rows outside the strict Ensembl window in staging.</div>`
          )
          .join("")}
        ${sourceCounts
          .map(
            (row) => `<div class="note-item">${escapeHtml(row.source_name)}: ${Number(row.row_count || 0).toLocaleString()} harmonized BRCA rows.</div>`
          )
          .join("")}
      </div>
    </details>
  `;
}

function renderInlineEvidenceTable(sample) {
  const columns = sample?.columns || [];
  const rows = sample?.rows || [];
  if (!columns.length) {
    return `<div class="empty-state">No evidence rows bundled for this source.</div>`;
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

  return `
    <div class="sample-table-wrap">
      <table class="sample-table live-table compact-table">
        <thead><tr>${head}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function renderSourceEvidenceList(source) {
  const evidenceLines = [
    `Source version: ${source.source_version || "not recorded"}`,
    `Raw vault prefix: ${source.raw_vault_prefix || "not recorded"}`,
    ...source.notes,
  ];
  return renderNoteStack(evidenceLines);
}

function renderSourceArtifactLinks(source) {
  const links = source.artifact_links || [];
  if (!links.length) {
    return `<div class="metric-sub">No additional frozen artifact links were recorded for this source.</div>`;
  }
  return renderLinkList(links);
}

function renderSourceLiftoverMethod(source) {
  if (!source.liftover_method) {
    return "";
  }

  const method = source.liftover_method;
  const counts = method.counts || {};
  const officialLinks = (method.official_sources || []).map((url, index) => ({
    label: `Method source ${index + 1}`,
    url,
  }));
  const failureNotes = (method.failure_examples || []).map(
    (example) =>
      `row ${example.source_row_number}: ${example.hgvs_genomic_grch37 || "missing"} -> ${example.liftover_status} (${example.liftover_notes})`
  );

  return `
    <details class="details-card" open>
      <summary>GRCh37 -> GRCh38 method</summary>
      <div class="metric-stack">
        <div class="metric-item">
          <div class="metric-title">Why liftover was required</div>
          <div class="metric-sub">${escapeHtml(method.why_needed)}</div>
        </div>
        <div class="metric-item">
          <div class="metric-title">How the mapping was executed</div>
          ${renderNoteStack(method.how_it_worked || [])}
        </div>
        <div class="metric-item">
          <div class="metric-title">Result counts</div>
          ${renderNoteStack([
            `parsed rows: ${Number(counts.parse_success_rows || 0).toLocaleString()} / ${Number(counts.total_rows || 0).toLocaleString()}`,
            `liftover success rows: ${Number(counts.liftover_success_rows || 0).toLocaleString()}`,
            `liftover failure rows: ${Number(counts.liftover_failure_rows || 0).toLocaleString()}`,
            `BRCA rows in source: ${Number(counts.brca_rows || 0).toLocaleString()}`,
            `BRCA rows lifted successfully: ${Number(counts.brca_liftover_success_rows || 0).toLocaleString()}`,
          ])}
        </div>
        <div class="metric-item">
          <div class="metric-title">Method summary</div>
          <div class="metric-sub">${escapeHtml(method.workflow_summary)}</div>
        </div>
        <div class="metric-item">
          <div class="metric-title">Official mapping references</div>
          ${renderLinkList(officialLinks)}
        </div>
        ${
          failureNotes.length
            ? `<div class="metric-item">
                <div class="metric-title">Failure examples</div>
                ${renderNoteStack(failureNotes)}
              </div>`
            : ""
        }
      </div>
    </details>
  `;
}

function renderSourceWorkflowPosition(source) {
  const workflow = source.workflow_position || {};
  const inclusionLabel = workflow.included_in_current_final ? "Already in current final checkpoint" : "Not in current final checkpoint";

  return `
    <div class="lineage-strip">
      <article class="lineage-step">
        <div class="lineage-label">Raw</div>
        <div class="lineage-value">${escapeHtml(workflow.raw_stage || "Not recorded")}</div>
      </article>
      <article class="lineage-step">
        <div class="lineage-label">BRCA</div>
        <div class="lineage-value">${escapeHtml(workflow.brca_stage || "Not recorded")}</div>
      </article>
      <article class="lineage-step">
        <div class="lineage-label">Final</div>
        <div class="lineage-value">${escapeHtml(workflow.final_stage || "Not recorded")}</div>
        <div class="lineage-flag">${escapeHtml(inclusionLabel)}</div>
      </article>
    </div>
    <details class="details-card">
      <summary>Exact workflow wording</summary>
      <div class="trace-grid">
        <article class="trace-item">
          <div class="trace-label">Stage 1</div>
          <div class="trace-value">${escapeHtml(workflow.raw_stage || "Not recorded")}</div>
        </article>
        <article class="trace-item">
          <div class="trace-label">Stage 2</div>
          <div class="trace-value">${escapeHtml(workflow.brca_stage || "Not recorded")}</div>
        </article>
        <article class="trace-item">
          <div class="trace-label">Stage 3</div>
          <div class="trace-value">${escapeHtml(workflow.final_stage || "Not recorded")}</div>
        </article>
      </div>
    </details>
  `;
}

function renderSourceDecisionSummary(payload) {
  const root = document.getElementById("source-decision-summary");
  if (!root) {
    return;
  }
  const rows = payload.decision_summary || [];
  root.innerHTML = rows
    .map(
      (entry) => `
      <article class="workflow-card scientific-card">
        <div class="workflow-step">${escapeHtml(entry.label.toUpperCase())}</div>
        <div class="workflow-title">${escapeHtml(entry.summary)}</div>
        <p class="workflow-summary">${Number(entry.count || 0).toLocaleString()} source(s)</p>
        ${renderNoteStack((entry.members || []).map((member) => `Source: ${member}`))}
      </article>
    `
    )
    .join("");
}

// [AI-Agent: Codex]: Review Stage C - controlled-access cards separate approved public data from high-value datasets that still need DAC or portal approval.
function renderControlledAccess(payload) {
  const guidesRoot = document.getElementById("controlled-access-guides");
  const sourcesRoot = document.getElementById("controlled-access-sources");
  const browseRoot = document.getElementById("controlled-access-browse-only");

  guidesRoot.innerHTML = payload.process_guides
    .map(
      (guide) => `
      <article class="workflow-card scientific-card">
        <div class="workflow-step">${escapeHtml(guide.key.toUpperCase())}</div>
        <div class="workflow-title">${escapeHtml(guide.title)}</div>
        <p class="workflow-summary">${escapeHtml(guide.why_it_exists)}</p>
        <details class="details-card" open>
          <summary>Exact steps</summary>
          ${renderNoteStack(guide.steps)}
        </details>
        <details class="details-card">
          <summary>Official links</summary>
          ${renderLinkList(guide.official_links)}
        </details>
        <div class="metric-sub">${escapeHtml(guide.source_note)}</div>
      </article>
    `
    )
    .join("");

  sourcesRoot.innerHTML = payload.sources
    .map(
      (source) => `
      <article class="explorer-card scientific-source-card">
        <div class="card-head">
          <div>
            <div class="sample-title">${escapeHtml(source.display_name)}</div>
            <div class="metric-sub">${escapeHtml(source.country_or_region)} • ${escapeHtml(source.priority.replaceAll("_", " "))}</div>
            <div class="metric-sub">${escapeHtml(source.access_model.replaceAll("_", " "))}</div>
          </div>
          <div class="review-pill partial">${escapeHtml(source.process_guide.toUpperCase())}</div>
        </div>
        <div class="scientific-matrix">
          <div><strong>What data</strong><span>${escapeHtml(source.data_scope)}</span></div>
          <div><strong>Why it matters</strong><span>${escapeHtml(source.why_we_need_it)}</span></div>
          <div><strong>Release evidence</strong><span>${escapeHtml(source.official_release_evidence)}</span></div>
          <div><strong>Build note</strong><span>${escapeHtml(source.build_or_coordinate_note)}</span></div>
        </div>
        <details class="details-card" open>
          <summary>How to get access</summary>
          ${renderNoteStack(source.access_steps)}
        </details>
        <details class="details-card">
          <summary>Official source links</summary>
          ${renderLinkList(source.official_links)}
        </details>
        <details class="details-card">
          <summary>Decision for this project</summary>
          <div class="metric-sub">${escapeHtml(source.practical_decision)}</div>
        </details>
      </article>
    `
    )
    .join("");

  browseRoot.innerHTML = payload.browse_only_sources
    .map(
      (source) => `
      <article class="explorer-card">
        <div class="card-head">
          <div>
            <div class="sample-title">${escapeHtml(source.display_name)}</div>
            <div class="metric-sub">${escapeHtml(source.status.replaceAll("_", " "))}</div>
          </div>
          <a class="action-button secondary-link compact-link" href="${escapeHtml(source.url)}" target="_blank" rel="noreferrer">Open source</a>
        </div>
        <p class="card-summary">${escapeHtml(source.summary)}</p>
      </article>
    `
    )
    .join("");
}

// [AI-Agent: Codex]: Review Stage B - render one source card per dataset so build readiness, coordinate readiness, and next action remain visible together.
function renderSourceReviewGrid(payload) {
  const root = document.getElementById("source-review-grid");
  root.innerHTML = payload.sources
    .map(
      (source) => `
      <article class="explorer-card scientific-source-card">
        <div class="card-head">
          <div>
            <div class="sample-title">${escapeHtml(source.display_name)}</div>
            <div class="metric-sub">${escapeHtml(source.category)} • ${escapeHtml(source.source_kind)}</div>
            <div class="metric-sub">snapshot ${escapeHtml(source.snapshot_date || "n/a")} • rows ${Number(source.row_count || 0).toLocaleString()}</div>
          </div>
          <div class="review-pill ${escapeHtml(source.review_status)}">${escapeHtml(String(source.review_status).toUpperCase())}</div>
        </div>
        <div class="scientific-matrix">
          <div><strong>Build</strong><span>${escapeHtml(source.source_build)}</span></div>
          <div><strong>Coordinates</strong><span>${escapeHtml(source.coordinate_readiness)}</span></div>
          <div><strong>Liftover</strong><span>${escapeHtml(source.liftover_decision)}</span></div>
          <div><strong>Normalization</strong><span>${escapeHtml(source.normalization_decision)}</span></div>
          <div><strong>BRCA relevance</strong><span>${escapeHtml(source.brca_relevance)}</span></div>
          <div><strong>Current use</strong><span>${escapeHtml(source.use_tier_label || toTitle(source.project_fit || "not_set"))}</span></div>
          <div><strong>Use strength</strong><span>${escapeHtml(source.use_tier_summary || "")}</span></div>
          <div><strong>Upstream</strong><span class="truncate">${escapeHtml(source.upstream_url || "")}</span></div>
        </div>
        ${renderSourceWorkflowPosition(source)}
        ${renderTraceSummary(source.trace)}
        <details class="details-card">
          <summary>Why this source is kept or limited</summary>
          <div class="metric-sub">${escapeHtml(source.project_fit_note)}</div>
        </details>
        ${renderSourceLiftoverMethod(source)}
        <details class="details-card">
          <summary>Scientific rationale and evidence</summary>
          ${renderSourceEvidenceList(source)}
        </details>
        <details class="details-card">
          <summary>Frozen provenance artifacts</summary>
          ${renderSourceArtifactLinks(source)}
        </details>
        <details class="details-card">
          <summary>Next exact action</summary>
          <div class="metric-sub">${escapeHtml(source.next_action)}</div>
        </details>
        ${
          source.sample
            ? `<details class="details-card">
                <summary>Frozen evidence sample (${Number((source.sample.rows || []).length).toLocaleString()} rows)</summary>
                ${renderInlineEvidenceTable(source.sample)}
              </details>`
            : ""
        }
      </article>
    `
    )
    .join("");
}

function renderSourceCounts(metrics) {
  const sourceCounts = metrics.source_row_counts || [];
  const frozenAt = metrics.frozen_at || "frozen snapshot";
  return `
    <details class="details-card">
      <summary>Frozen source counts (${escapeHtml(frozenAt)})</summary>
      <div class="note-stack">
        ${sourceCounts
          .map(
            (row) => `<div class="note-item">${escapeHtml(row.source_name)}: ${Number(row.row_count || 0).toLocaleString()} rows in this checkpoint.</div>`
          )
          .join("")}
      </div>
    </details>
  `;
}

// [AI-Agent: Codex]: Section 3 / Frozen evidence rendering - every preview must state that it comes from the approved static bundle, not from a live analytical query.
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
      <summary>Frozen sample provenance</summary>
      <div class="query-meta">
        <div class="note-stack">
          <div class="note-item">Mode: ${escapeHtml(payload.mode || "frozen bundle")}</div>
          <div class="note-item">Frozen at: ${escapeHtml(payload.frozen_at || "not recorded")}</div>
        </div>
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
  document.getElementById(targetId).innerHTML = `<div class="loading-state">${escapeHtml(label)}</div>`;
}

function setError(targetId, error) {
  document.getElementById(targetId).innerHTML = `<div class="error-state">${escapeHtml(error.message || error)}</div>`;
}

async function runButtonAction(button, busyLabel, callback) {
  if (button.disabled) {
    return;
  }

  const originalLabel = button.dataset.originalLabel || button.textContent.trim();
  button.dataset.originalLabel = originalLabel;
  button.disabled = true;
  button.setAttribute("aria-busy", "true");
  button.textContent = busyLabel;

  try {
    await callback();
  } finally {
    button.disabled = false;
    button.setAttribute("aria-busy", "false");
    button.textContent = originalLabel;
  }
}

// [AI-Agent: Codex]: Section 4 / Interactive cards - cards surface frozen artifacts first, and only then show any historical build references used to create them.
function renderDatasetCollection({ targetId, payload, sampleAttr, samplePath }) {
  const root = document.getElementById(targetId);
  root.innerHTML = payload.datasets
    .map(
      (entry) => {
        const primaryRef = entry.storage_ref || entry.table_ref;
        const historicalRef = entry.storage_ref ? entry.table_ref : null;
        const downloadLink = entry.download_url
          ? `<a class="action-button secondary-link" href="${entry.download_url}">Download static file</a>`
          : "";
        return `
      <article class="explorer-card">
        <div class="card-head">
          <div>
            <div class="sample-title">${escapeHtml(entry.title)}</div>
            <div class="metric-sub">${escapeHtml(primaryRef)}</div>
            ${historicalRef ? `<div class="metric-sub">Historical build reference: ${escapeHtml(historicalRef)}</div>` : ""}
            <div class="metric-sub">frozen row count: ${(entry.row_count || 0).toLocaleString()}</div>
          </div>
          <div class="action-row compact-actions">
            <button type="button" class="action-button" data-${sampleAttr}="${entry.key}">Show frozen 10 rows</button>
            ${downloadLink}
          </div>
        </div>
        <p class="card-summary">${escapeHtml(entry.simple_summary)}</p>
        ${renderTraceSummary(entry.trace)}
        <details class="details-card">
          <summary>How this table is used</summary>
          ${renderNoteStack(entry.notes)}
        </details>
        <details class="details-card">
          <summary>Column glossary</summary>
          ${renderGlossary(entry.columns)}
        </details>
        <div id="${sampleAttr}-sample-${entry.key}" class="query-result"></div>
      </article>
    `;
      }
    )
    .join("");

  root.querySelectorAll(`[data-${sampleAttr}]`).forEach((button) => {
    button.addEventListener("click", async () => {
      const key = button.getAttribute(`data-${sampleAttr}`);
      const target = `${sampleAttr}-sample-${key}`;
      setLoading(target, "Loading frozen sample...");
      await runButtonAction(button, "Loading sample...", async () => {
        try {
          const samplePayload = await fetchJson(`${samplePath}/${key}/sample`);
          renderQueryResult(target, samplePayload);
        } catch (error) {
          setError(target, error);
        }
      });
    });
  });
}

function renderSupplementalRawSources(payload) {
  const root = document.getElementById("raw-source-package-explorer");
  const entries = (payload.sources || []).filter((source) => !(source.workflow_position?.raw_stage || "").includes("Raw page"));

  root.innerHTML = entries
    .map(
      (source) => `
      <article class="explorer-card">
        <div class="card-head">
          <div>
            <div class="sample-title">${escapeHtml(source.display_name)}</div>
            <div class="metric-sub">${escapeHtml(source.raw_vault_prefix || source.upstream_url || "raw source path not recorded")}</div>
            <div class="metric-sub">frozen row count: ${Number(source.row_count || 0).toLocaleString()}</div>
            <div class="metric-sub">Current use: ${escapeHtml(source.use_tier_label || "not recorded")}</div>
          </div>
        </div>
        <p class="card-summary">${escapeHtml(source.workflow_position?.raw_stage || "Frozen source package")}</p>
        ${renderTraceSummary(source.trace)}
        <details class="details-card">
          <summary>Why this raw package matters</summary>
          <div class="metric-sub">${escapeHtml(source.project_fit_note || "Not recorded")}</div>
        </details>
        <details class="details-card">
          <summary>Frozen provenance artifacts</summary>
          ${renderSourceArtifactLinks(source)}
        </details>
        ${
          source.sample
            ? `<details class="details-card">
                <summary>Frozen evidence sample (${Number((source.sample.rows || []).length).toLocaleString()} rows)</summary>
                ${renderInlineEvidenceTable(source.sample)}
              </details>`
            : ""
        }
      </article>
    `
    )
    .join("");
}

function renderStepCards(targetId, steps) {
  const root = document.getElementById(targetId);
  root.innerHTML = steps
    .map(
      (step) => `
      <article class="explorer-card">
        <div class="card-head">
          <div>
            <div class="sample-title">${escapeHtml(step.title)}</div>
            <div class="metric-sub">${escapeHtml(step.simple)}</div>
          </div>
          <div class="action-row compact-actions">
            <button type="button" class="action-button secondary" data-step-sample="${step.id}">Show frozen 10 rows</button>
          </div>
        </div>
        ${renderTraceSummary(step.trace)}
        <details class="details-card">
          <summary>Technical note</summary>
          <div class="metric-sub">${escapeHtml(step.technical)}</div>
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
      setLoading(target, "Loading frozen step sample...");
      await runButtonAction(button, "Loading sample...", async () => {
        try {
          const payload = await fetchJson(`/api/registry/steps/${stepId}/sample`);
          renderQueryResult(target, payload);
        } catch (error) {
          setError(target, error);
        }
      });
    });
  });
}

function renderHarmonizationScience(payload) {
  const root = document.getElementById("harmonization-science");
  root.innerHTML = `
    <article class="metric-item">
      <div class="metric-title">Scientific method for BRCA checkpoint extraction</div>
      <div class="metric-sub">${escapeHtml(payload.scope_note)}</div>
      ${renderNoteStack(payload.accuracy_notes)}
      <details class="details-card">
        <summary>Scientific explanation</summary>
        ${renderNoteStack(payload.scientific_notes)}
      </details>
      ${payload.scientific_metrics ? renderScientificMetrics(payload.scientific_metrics) : ""}
    </article>
  `;
}

// [AI-Agent: Codex]: Section 5 / Stage-specific metadata panels - keep the same narrative order: what exists, why it exists, then the field-level contract.
function renderPreGmeMeta(payload) {
  const root = document.getElementById("pre-gme-meta");
  root.innerHTML = `
    <article class="metric-item">
      <div class="metric-title">${escapeHtml(payload.title)}</div>
      <div class="metric-sub">${escapeHtml(payload.table_ref)}</div>
      <div class="metric-sub">frozen row count: ${payload.row_count === null ? "not built yet" : Number(payload.row_count).toLocaleString()}</div>
      <div class="metric-sub">${escapeHtml(payload.scope_note)}</div>
      ${renderTraceSummary(payload.trace)}
      ${renderNoteStack(payload.accuracy_notes)}
      <details class="details-card">
        <summary>Why this checkpoint exists</summary>
        ${renderNoteStack(payload.scientific_notes)}
      </details>
      ${payload.scientific_metrics ? renderSourceCounts(payload.scientific_metrics) : ""}
    </article>
  `;

  document.getElementById("pre-gme-columns").innerHTML = renderGlossary(payload.columns);
  document.getElementById("pre-gme-build-sql").textContent = payload.build_sql;
  document.getElementById("pre-gme-metadata-preview").textContent = payload.export_metadata_preview.join("\n");
  const kindByName = Object.fromEntries(payload.columns.map((column) => [column.name, column.kind || "required"]));
  document.getElementById("pre-gme-header-preview").innerHTML = payload.export_header_columns
    .map((column) => `<div class="header-chip ${kindByName[column] || "required"}">${escapeHtml(column)}</div>`)
    .join("");
}

function renderFinalRegistryMeta(payload) {
  const root = document.getElementById("registry-meta");
  root.innerHTML = `
    <article class="metric-item">
      <div class="metric-title">${escapeHtml(payload.title)}</div>
      <div class="metric-sub">${escapeHtml(payload.table_ref)}</div>
      <div class="metric-sub">frozen row count: ${payload.row_count === null ? "not built yet" : Number(payload.row_count).toLocaleString()}</div>
      <div class="metric-sub">${escapeHtml(payload.scope_note)}</div>
      ${renderTraceSummary(payload.trace)}
      ${renderNoteStack(payload.accuracy_notes)}
      <details class="details-card">
        <summary>Scientific explanation</summary>
        ${renderNoteStack(payload.scientific_notes)}
      </details>
      ${payload.scientific_metrics ? renderScientificMetrics(payload.scientific_metrics) : ""}
    </article>
  `;

  document.getElementById("registry-columns").innerHTML = renderGlossary(payload.columns);
  document.getElementById("registry-build-sql").textContent = payload.build_sql;
  document.getElementById("registry-download-csv-link").setAttribute("href", payload.csv_download_url);
}

function renderOverviewHeader(payload) {
  document.getElementById("generated-at").textContent = `Supervisor state refreshed: ${payload.generated_at}`;
  document.getElementById("last-successful-step").textContent = payload.last_successful_step
    ? `Latest confirmed checkpoint: ${payload.last_successful_step}`
    : "Latest confirmed checkpoint: not recorded yet.";
}

function renderOverviewPage() {
  renderStatusCards(overviewPayload.track_status_counts);
  renderTrackGrid(overviewPayload);
  renderRuntimeResults(overviewPayload.latest_t002_verification);
  renderWorkflowMap(workflowPages());
}

async function loadShellData() {
  const [overview, workflow] = await Promise.all([
    fetchResource("overview", "/api/overview"),
    fetchResource("workflow", "/api/workflow"),
  ]);

  overviewPayload = overview;
  workflowPayload = workflow;
  renderWorkflowNav(workflowPages());
  renderOverviewHeader(overview);
}

// [AI-Agent: Codex]: Section 6 / Button wiring and lazy page loaders - keep the startup cheap, then fetch only the frozen bundle slice needed for the active page.
function wireGlobalButtons() {
  if (globalButtonsWired) {
    return;
  }

  const preGmeButton = document.getElementById("pre-gme-sample-button");
  const registryButton = document.getElementById("registry-sample-button");

  preGmeButton.addEventListener("click", async () => {
    const target = "pre-gme-sample";
    setLoading(target, "Loading frozen pre-GME sample...");
    await runButtonAction(preGmeButton, "Loading sample...", async () => {
      try {
        const payload = await fetchJson("/api/pre-gme/sample");
        renderQueryResult(target, payload);
      } catch (error) {
        setError(target, error);
      }
    });
  });

  registryButton.addEventListener("click", async () => {
    const target = "registry-sample";
    setLoading(target, "Loading frozen final-registry sample...");
    await runButtonAction(registryButton, "Loading sample...", async () => {
      try {
        const payload = await fetchJson("/api/registry/sample");
        renderQueryResult(target, payload);
      } catch (error) {
        setError(target, error);
      }
    });
  });

  globalButtonsWired = true;
}

async function loadOverviewPage() {
  renderOverviewPage();
  renderedPages.add("overview");
}

async function loadRawPage() {
  if (renderedPages.has("raw")) {
    return;
  }

  setLoading("raw-dataset-explorer", "Loading frozen raw dataset catalog...");
  setLoading("raw-source-package-explorer", "Loading additional frozen source packages...");
  const [rawPayload, sourceReviewPayload] = await Promise.all([
    fetchResource("raw-datasets", "/api/raw-datasets"),
    fetchResource("source-review", "/api/source-review"),
  ]);
  renderDatasetCollection({
    targetId: "raw-dataset-explorer",
    payload: rawPayload,
    sampleAttr: "raw-sample",
    samplePath: "/api/raw-datasets",
  });
  renderSupplementalRawSources(sourceReviewPayload);
  renderedPages.add("raw");
}

async function loadHarmonizationPage() {
  if (renderedPages.has("harmonization")) {
    return;
  }

  setLoading("workflow-categories", "Loading workflow categories...");
  setLoading("source-decision-summary", "Loading source decisions...");
  setLoading("source-review-grid", "Loading scientific source review...");
  setLoading("harmonization-science", "Loading frozen scientific checkpoint evidence...");
  setLoading("harmonized-dataset-explorer", "Loading frozen checkpoint tables...");
  setLoading("harmonization-steps", "Loading frozen evidence steps...");

  const [harmonizedPayload, registryPayload, sourceReviewPayload] = await Promise.all([
    fetchResource("checkpoint-datasets", "/api/datasets"),
    fetchResource("registry-meta", "/api/registry"),
    fetchResource("source-review", "/api/source-review"),
  ]);

  renderWorkflowCategories(sourceReviewPayload.workflow_categories);
  renderSourceDecisionSummary(sourceReviewPayload);
  renderSourceReviewGrid(sourceReviewPayload);
  renderHarmonizationScience(registryPayload);
  renderDatasetCollection({
    targetId: "harmonized-dataset-explorer",
    payload: harmonizedPayload,
    sampleAttr: "harmonized-sample",
    samplePath: "/api/datasets",
  });
  renderStepCards("harmonization-steps", workflowPayload.harmonization_steps);
  renderedPages.add("harmonization");
}

async function loadAccessPage() {
  if (renderedPages.has("access")) {
    return;
  }

  setLoading("controlled-access-summary", "Loading controlled-access summary...");
  setLoading("controlled-access-guides", "Loading official process guides...");
  setLoading("controlled-access-sources", "Loading source-specific access cards...");
  setLoading("controlled-access-browse-only", "Loading browse-only notes...");

  const payload = await fetchResource("controlled-access", "/api/controlled-access");
  document.getElementById("controlled-access-summary").innerHTML = `
    <article class="metric-item">
      <div class="metric-title">Controlled-access acquisition roadmap</div>
      <div class="metric-sub">${escapeHtml(payload.scope_note)}</div>
      ${renderNoteStack([payload.decision_note])}
    </article>
  `;
  renderControlledAccess(payload);
  renderedPages.add("access");
}

async function loadPreGmePage() {
  if (renderedPages.has("pre-gme")) {
    return;
  }

  setLoading("pre-gme-meta", "Loading frozen pre-GME checkpoint metadata...");
  const payload = await fetchResource("pre-gme-meta", "/api/pre-gme");
  renderPreGmeMeta(payload);
  renderedPages.add("pre-gme");
}

async function loadFinalPage() {
  if (renderedPages.has("final")) {
    return;
  }

  setLoading("registry-meta", "Loading frozen final checkpoint metadata...");
  setLoading("final-steps", "Loading frozen final-stage evidence...");
  const payload = await fetchResource("registry-meta", "/api/registry");
  renderFinalRegistryMeta(payload);
  renderStepCards("final-steps", workflowPayload.final_steps);
  renderedPages.add("final");
}

async function loadActivePage(pageId) {
  try {
    if (pageId === "overview") {
      await loadOverviewPage();
      return;
    }
    if (pageId === "raw") {
      await loadRawPage();
      return;
    }
    if (pageId === "harmonization") {
      await loadHarmonizationPage();
      return;
    }
    if (pageId === "access") {
      await loadAccessPage();
      return;
    }
    if (pageId === "pre-gme") {
      await loadPreGmePage();
      return;
    }
    if (pageId === "final") {
      await loadFinalPage();
    }
  } catch (error) {
    if (pageId === "raw") setError("raw-dataset-explorer", error);
    if (pageId === "harmonization") {
      setError("workflow-categories", error);
      setError("source-review-grid", error);
      setError("harmonization-science", error);
      setError("harmonized-dataset-explorer", error);
      setError("harmonization-steps", error);
    }
    if (pageId === "access") {
      setError("controlled-access-summary", error);
      setError("controlled-access-guides", error);
      setError("controlled-access-sources", error);
      setError("controlled-access-browse-only", error);
    }
    if (pageId === "pre-gme") setError("pre-gme-meta", error);
    if (pageId === "final") {
      setError("registry-meta", error);
      setError("final-steps", error);
    }
  }
}

async function navigate() {
  if (!workflowPayload || !overviewPayload) {
    return;
  }
  const pageId = currentPageId();
  activatePage(pageId);
  await loadActivePage(pageId);
}

async function main() {
  const generatedAtNode = document.getElementById("generated-at");
  try {
    await loadShellData();
    wireGlobalButtons();
    await navigate();
  } catch (error) {
    generatedAtNode.textContent = `Failed to load live supervisor state: ${error.message}`;
    document.getElementById("last-successful-step").textContent = "";
  }
}

window.addEventListener("hashchange", () => {
  void navigate();
});

void main();
