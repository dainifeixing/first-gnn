const DEFAULT_MANIFEST = "./showcase_manifest.json";

const tierLabels = {
  all: "All Tiers",
  strong_bridge_unknown_seller: "Strong Bridge",
  weak_bridge_high_score: "Weak Bridge",
  high_support_non_bridge: "High Support",
  score_only: "Score Only",
};

const viewLabels = {
  public: "Presentation",
  internal: "Internal",
};

const state = {
  manifest: null,
  showcaseIndex: null,
  roundName: "",
  tier: "all",
  query: "",
  selectedSeller: "",
  viewMode: "public",
  presentationIndex: 0,
  autoplayTimer: null,
  presentationLayout: false,
};

function safeText(value) {
  return String(value ?? "");
}

function escapeHtml(value) {
  return safeText(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function badge(label) {
  const normalized = safeText(label).trim().toLowerCase();
  if (!normalized) {
    return "<span class='badge'>unreviewed</span>";
  }
  if (normalized === "confirmed_positive") {
    return "<span class='badge badge-positive'>confirmed_positive</span>";
  }
  if (normalized === "confirmed_negative") {
    return "<span class='badge badge-negative'>confirmed_negative</span>";
  }
  if (normalized === "uncertain") {
    return "<span class='badge badge-uncertain'>uncertain</span>";
  }
  return `<span class='badge'>${escapeHtml(normalized)}</span>`;
}

function shortPath(value) {
  const text = safeText(value).replaceAll("\\", "/");
  const parts = text.split("/").filter(Boolean);
  return parts.length ? parts.slice(-2).join("/") : text;
}

function maskToken(value, left = 3, right = 3) {
  const text = safeText(value).trim();
  if (!text) {
    return "";
  }
  if (text.length <= left + right) {
    return `${text.slice(0, 1)}***`;
  }
  return `${text.slice(0, left)}***${text.slice(-right)}`;
}

function maskAccount(value) {
  const text = safeText(value).trim();
  if (!text) {
    return "";
  }
  if (text.includes("@")) {
    const [local, domain] = text.split("@");
    return `${maskToken(local, 2, 2)}@${domain}`;
  }
  return maskToken(text, 3, 2);
}

function maskName(value) {
  const text = safeText(value).trim();
  if (!text) {
    return "";
  }
  if (text.length <= 2) {
    return `${text.slice(0, 1)}*`;
  }
  return `${text.slice(0, 1)}***${text.slice(-1)}`;
}

function present(value, kind = "text") {
  const text = safeText(value).trim();
  if (state.viewMode === "internal") {
    return text || "-";
  }
  if (!text) {
    return "-";
  }
  if (kind === "account") {
    return maskAccount(text);
  }
  if (kind === "name") {
    return maskName(text);
  }
  if (kind === "note") {
    return "hidden in presentation mode";
  }
  if (kind === "path") {
    return shortPath(text);
  }
  return maskToken(text, 2, 2);
}

function currentCandidates() {
  const candidates = state.manifest?.seller_candidates ?? [];
  return candidates.filter((item) => {
    const tierOk = state.tier === "all" || safeText(item.candidate_tier) === state.tier;
    const query = state.query.trim().toLowerCase();
    const haystack = [
      item.seller_account,
      ...(item.sample_counterparties || []),
      item.review_label,
      item.review_note,
    ]
      .join(" ")
      .toLowerCase();
    const queryOk = !query || haystack.includes(query);
    return tierOk && queryOk;
  });
}

function queueCandidates() {
  return currentCandidates().slice(0, 5);
}

function currentRoundEntries() {
  return state.showcaseIndex?.rounds || [];
}

function currentRoundEntry() {
  return currentRoundEntries().find((item) => item.round_name === state.roundName) || null;
}

function previousRoundEntry() {
  const rounds = currentRoundEntries();
  const currentIndex = rounds.findIndex((item) => item.round_name === state.roundName);
  if (currentIndex <= 0) {
    return null;
  }
  return rounds[currentIndex - 1] || null;
}

function stopAutoplay() {
  if (state.autoplayTimer) {
    window.clearInterval(state.autoplayTimer);
    state.autoplayTimer = null;
  }
}

function flashPanel(target) {
  if (!target) {
    return;
  }
  const panel = document.querySelector(`[data-panel="${target}"]`);
  if (!panel) {
    return;
  }
  panel.classList.add("panel-focus-flash");
  panel.scrollIntoView({ behavior: "smooth", block: "start" });
  window.setTimeout(() => {
    panel.classList.remove("panel-focus-flash");
  }, 1800);
}

function handleCollaborationJump(target, tier, seller) {
  stopAutoplay();
  if (tier) {
    state.tier = tier;
  }
  if (seller && (state.manifest?.seller_candidates || []).some((item) => item.seller_account === seller)) {
    state.selectedSeller = seller;
  } else if (tier && !currentCandidates().some((item) => item.seller_account === state.selectedSeller)) {
    state.selectedSeller = currentCandidates()[0]?.seller_account || "";
  }
  render();
  flashPanel(target);
}

async function setPresentationLayout(nextValue) {
  state.presentationLayout = Boolean(nextValue);
  document.body.classList.toggle("presentation-layout", state.presentationLayout);
  try {
    if (state.presentationLayout && document.fullscreenEnabled && !document.fullscreenElement) {
      await document.documentElement.requestFullscreen();
    } else if (!state.presentationLayout && document.fullscreenElement) {
      await document.exitFullscreen();
    }
  } catch (error) {
    // Ignore fullscreen API failures and keep CSS-only presentation mode.
  }
}

function syncPresentationIndex() {
  const queue = queueCandidates();
  const matchedIndex = queue.findIndex((item) => item.seller_account === state.selectedSeller);
  state.presentationIndex = matchedIndex >= 0 ? matchedIndex : 0;
}

function selectPresentationIndex(index) {
  const queue = queueCandidates();
  if (!queue.length) {
    state.presentationIndex = 0;
    state.selectedSeller = "";
    return;
  }
  const normalizedIndex = ((index % queue.length) + queue.length) % queue.length;
  state.presentationIndex = normalizedIndex;
  state.selectedSeller = queue[normalizedIndex].seller_account;
}

function renderRoundChips() {
  const container = document.getElementById("round-chips");
  const rounds = currentRoundEntries();
  if (!rounds.length) {
    container.innerHTML = "<span class='chip secondary-active'>current</span>";
    return;
  }
  container.innerHTML = rounds
    .map(
      (item) => `
        <button class="chip ${state.roundName === item.round_name ? "secondary-active" : ""}" data-round-name="${escapeHtml(item.round_name)}">
          ${escapeHtml(item.title || item.round_name)}
        </button>
      `
    )
    .join("");
  container.querySelectorAll("[data-round-name]").forEach((button) => {
    button.addEventListener("click", () => {
      const nextRound = button.dataset.roundName || "";
      const entry = currentRoundEntries().find((item) => item.round_name === nextRound);
      if (!entry || !entry.manifest) {
        return;
      }
      stopAutoplay();
      state.roundName = nextRound;
      state.manifest = entry.manifest;
      state.selectedSeller = state.manifest?.seller_candidates?.[0]?.seller_account || "";
      state.tier = "all";
      state.query = "";
      const url = new URL(window.location.href);
      url.searchParams.set("round", nextRound);
      window.history.replaceState({}, "", url);
      const filter = document.getElementById("seller-filter");
      if (filter) {
        filter.value = "";
      }
      render();
    });
  });
}

function renderModeChips() {
  const container = document.getElementById("mode-chips");
  container.innerHTML = Object.entries(viewLabels)
    .map(
      ([key, label]) => `
        <button class="chip ${state.viewMode === key ? "secondary-active" : ""}" data-view-mode="${key}">
          ${escapeHtml(label)}
        </button>
      `
    )
    .join("");
  container.querySelectorAll("[data-view-mode]").forEach((button) => {
    button.addEventListener("click", () => {
      state.viewMode = button.dataset.viewMode || "public";
      const url = new URL(window.location.href);
      url.searchParams.set("view", state.viewMode);
      window.history.replaceState({}, "", url);
      render();
    });
  });
}

function renderPresentationControls() {
  const container = document.getElementById("presentation-controls");
  const queue = queueCandidates();
  if (!queue.length) {
    container.innerHTML = "";
    return;
  }
  const isPlaying = Boolean(state.autoplayTimer);
  container.innerHTML = `
    <button class="chip" data-presentation-action="prev">Previous</button>
    <button class="chip" data-presentation-action="next">Next</button>
    <button class="chip ${isPlaying ? "secondary-active" : ""}" data-presentation-action="toggle">
      ${isPlaying ? "Stop Auto" : "Auto Play"}
    </button>
    <button class="chip ${state.presentationLayout ? "secondary-active" : ""}" data-presentation-action="layout">
      ${state.presentationLayout ? "Exit Stage" : "Stage Mode"}
    </button>
    <span class="chip secondary-active">Step ${state.presentationIndex + 1}/${queue.length}</span>
  `;
  container.querySelectorAll("[data-presentation-action]").forEach((button) => {
    button.addEventListener("click", async () => {
      const action = button.dataset.presentationAction || "";
      if (action === "prev") {
        stopAutoplay();
        selectPresentationIndex(state.presentationIndex - 1);
      } else if (action === "next") {
        stopAutoplay();
        selectPresentationIndex(state.presentationIndex + 1);
      } else if (action === "toggle") {
        if (state.autoplayTimer) {
          stopAutoplay();
        } else {
          state.autoplayTimer = window.setInterval(() => {
            selectPresentationIndex(state.presentationIndex + 1);
            render();
          }, 3500);
        }
      } else if (action === "layout") {
        await setPresentationLayout(!state.presentationLayout);
      }
      render();
    });
  });
}

function executiveSummaryData() {
  const manifest = state.manifest || {};
  const previousEntry = previousRoundEntry();
  const previousManifest = previousEntry?.manifest || null;
  const currentSummary = manifest.score_summary || {};
  const currentRecovery = manifest.seller_recovery || {};
  const currentMetrics = manifest.report_metrics || {};
  const previousSummary = previousManifest?.score_summary || {};
  const previousRecovery = previousManifest?.seller_recovery || {};
  const previousMetrics = previousManifest?.report_metrics || {};
  const currentStrong = Number(currentSummary.strong_bridge_candidates || 0);
  const previousStrong = Number(previousSummary.strong_bridge_candidates || 0);
  const currentRecoveryRate = Number(currentRecovery.recovery_rate || 0);
  const previousRecoveryRate = Number(previousRecovery.recovery_rate || 0);
  const currentValF1 = Number(currentMetrics.best_val_f1 || 0);
  const previousValF1 = Number(previousMetrics.best_val_f1 || 0);
  const topCandidate = manifest.seller_candidates?.[0] || null;
  const bullets = [
    `Current round exposes ${Number(currentSummary.returned_seller_candidates || manifest.seller_candidates?.length || 0)} seller candidates, with ${currentStrong} in the strong-bridge tier.`,
    `Frozen-eval seller recovery is ${currentRecoveryRate.toFixed(4)} and validation F1 is ${currentValF1.toFixed(4)}.`,
  ];
  if (previousManifest) {
    bullets.push(
      `Compared with ${previousEntry.round_name}, strong-bridge candidates changed by ${(currentStrong - previousStrong) >= 0 ? "+" : ""}${currentStrong - previousStrong}, seller recovery changed by ${(currentRecoveryRate - previousRecoveryRate).toFixed(4)}, and validation F1 changed by ${(currentValF1 - previousValF1).toFixed(4)}.`
    );
  }
  if (topCandidate) {
    bullets.push(
      `The first walkthrough candidate is ${present(topCandidate.seller_account, "account")}, supported by ${topCandidate.bridge_buyers} bridge buyers and uplift ${Number(topCandidate.bridge_uplift || 0).toFixed(4)}.`
    );
  }
  const topBridge = safeText(currentSummary.top_bridge_seller_candidate || "");
  return {
    bullets,
    talkTrack: [
      "Start with the strong-bridge queue, then explain how bridge buyers connect known seller seeds to the current unknown seller candidate set.",
      `Highest bridge anchor in this round: ${present(topBridge, "account")}. Use the Extension Story section to narrate one seller at a time.`,
    ],
  };
}

function executiveRoundRows() {
  const rounds = currentRoundEntries();
  if (!rounds.length && state.manifest) {
    return [
      {
        round_name: state.manifest?.meta?.round_name || "current",
        title: state.manifest?.title || state.manifest?.meta?.round_name || "current",
        manifest: state.manifest,
      },
    ].map((entry) => {
      const manifest = entry.manifest || {};
      const summary = manifest.score_summary || {};
      const recovery = manifest.seller_recovery || {};
      const metrics = manifest.report_metrics || {};
      const topCandidate = manifest.seller_candidates?.[0] || null;
      return {
        round_name: entry.round_name,
        title: entry.title,
        strong_bridge_candidates: Number(summary.strong_bridge_candidates || 0),
        returned_seller_candidates: Number(summary.returned_seller_candidates || manifest.seller_candidates?.length || 0),
        bridge_candidate_rate: Number(summary.bridge_candidate_rate || 0),
        seller_recovery_rate: Number(recovery.recovery_rate || 0),
        best_val_f1: Number(metrics.best_val_f1 || 0),
        top_candidate: topCandidate
          ? {
              seller_account: present(topCandidate.seller_account, "account"),
              bridge_buyers: Number(topCandidate.bridge_buyers || 0),
              bridge_uplift: Number(topCandidate.bridge_uplift || 0),
              candidate_tier: topCandidate.candidate_tier || "unknown",
            }
          : null,
      };
    });
  }
  return rounds.map((entry) => {
    const manifest = entry.manifest || {};
    const summary = manifest.score_summary || {};
    const recovery = manifest.seller_recovery || {};
    const metrics = manifest.report_metrics || {};
    const topCandidate = manifest.seller_candidates?.[0] || null;
    return {
      round_name: entry.round_name,
      title: entry.title || entry.round_name,
      strong_bridge_candidates: Number(summary.strong_bridge_candidates || 0),
      returned_seller_candidates: Number(summary.returned_seller_candidates || manifest.seller_candidates?.length || 0),
      bridge_candidate_rate: Number(summary.bridge_candidate_rate || 0),
      seller_recovery_rate: Number(recovery.recovery_rate || 0),
      best_val_f1: Number(metrics.best_val_f1 || 0),
      top_candidate: topCandidate
        ? {
            seller_account: present(topCandidate.seller_account, "account"),
            bridge_buyers: Number(topCandidate.bridge_buyers || 0),
            bridge_uplift: Number(topCandidate.bridge_uplift || 0),
            candidate_tier: topCandidate.candidate_tier || "unknown",
          }
        : null,
    };
  });
}

function presenterScriptLines() {
  const manifest = state.manifest || {};
  const item = (manifest.seller_candidates || []).find((entry) => entry.seller_account === state.selectedSeller);
  if (!item) {
    return { item: null, scriptLines: [] };
  }
  const previousEntry = previousRoundEntry();
  const previousManifest = previousEntry?.manifest || null;
  const previousMatch = previousManifest?.seller_candidates?.find((entry) => entry.seller_account === item.seller_account) || null;
  const upliftDelta = previousMatch ? Number(item.bridge_uplift || 0) - Number(previousMatch.bridge_uplift || 0) : null;
  const buyerDelta = previousMatch ? Number(item.bridge_buyers || 0) - Number(previousMatch.bridge_buyers || 0) : null;
  const topBuyers = (item.buyer_support || [])
    .slice(0, 2)
    .map((row) => present(row.buyer_account, "account"))
    .join("、") || "暂无";
  const scriptLines = [
    `这一页我重点讲候选 ${present(item.seller_account, "account")}。`,
    `它当前位于 ${item.candidate_tier || "unknown"} 层，bridge buyers 数量是 ${item.bridge_buyers || 0}，bridge uplift 是 ${Number(item.bridge_uplift || 0).toFixed(4)}。`,
    `支撑它的关键买方主要包括 ${topBuyers}，这些买方之前已经和已知 seller 种子发生过联系，所以它不是孤立高分，而是有扩线桥接证据。`,
    `从交易面上看，这个候选目前有 ${item.support_rows || 0} 条支持交易，覆盖 ${item.unique_buyers || 0} 个买方，桥接比例 ${Number(item.bridge_support_ratio || 0).toFixed(2)}。`,
  ];
  if (previousMatch) {
    scriptLines.push(
      `和上一轮 ${previousEntry.round_name} 相比，它的 bridge uplift 变化 ${(upliftDelta >= 0 ? "+" : "") + upliftDelta.toFixed(4)}，bridge buyers 变化 ${(buyerDelta >= 0 ? "+" : "") + buyerDelta}。`
    );
  } else if (previousEntry) {
    scriptLines.push(`上一轮 ${previousEntry.round_name} 里，这个候选没有进入当前展示队列，所以它可以视为这一轮更值得讲的新优先对象。`);
  }
  scriptLines.push("汇报时建议先讲 bridge 证据，再讲支持交易数量，最后落到人工复核优先级。");
  return { item, scriptLines };
}

function buildExecutiveBriefingHtml() {
  const manifest = state.manifest || {};
  const summary = executiveSummaryData();
  const roundRows = executiveRoundRows();
  const recommendations = (manifest.recommendations || []).slice(0, 5);
  const topQueue = queueCandidates().slice(0, 5);
  const currentRound = currentRoundEntry();
  const previousRound = previousRoundEntry();
  const collaboration = manifest.collaboration || {};
  const collaborationAgents = collaboration.agents || [];
  const collaborationTimeline = collaboration.timeline || [];
  const collaborationAgreements = collaboration.agreements || [];
  const collaborationConflicts = collaboration.conflicts || [];
  const collaborationAdoptedChanges = collaboration.adopted_changes || [];
  const generatedAt = new Date().toISOString().replace("T", " ").slice(0, 19);
  return `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${escapeHtml(manifest.title || "txflow-risk executive briefing")}</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f1e7;
      --panel: #ffffff;
      --line: rgba(109, 85, 45, 0.14);
      --text: #1f2328;
      --muted: #66707a;
      --accent: #9f4e2f;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 28px;
      font-family: "Segoe UI", "PingFang SC", "Noto Sans SC", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(248, 205, 145, 0.22), transparent 28%),
        linear-gradient(180deg, #fcfbf7 0%, var(--bg) 100%);
    }
    .sheet {
      max-width: 1120px;
      margin: 0 auto;
      display: grid;
      gap: 18px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 22px 24px;
      box-shadow: 0 14px 34px rgba(86, 63, 33, 0.08);
      break-inside: avoid;
    }
    .hero-grid {
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 16px;
    }
    .meta-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-top: 12px;
    }
    .meta-card {
      padding: 14px 16px;
      border-radius: 16px;
      background: #faf5ec;
      border: 1px solid var(--line);
    }
    .meta-card strong {
      display: block;
      margin-bottom: 6px;
      font-size: 12px;
      color: #7c6340;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .eyebrow {
      margin: 0 0 8px;
      color: #7c6340;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 12px;
    }
    h1, h2, h3 { margin: 0; letter-spacing: -0.03em; }
    h1 { font-size: 34px; }
    h2 { font-size: 22px; margin-bottom: 12px; }
    h3 { font-size: 16px; margin-bottom: 8px; }
    p, li { line-height: 1.65; }
    .muted { color: var(--muted); }
    ul { margin: 0; padding-left: 18px; }
    li + li { margin-top: 8px; }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }
    th, td {
      text-align: left;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }
    th {
      color: var(--muted);
      font-weight: 600;
      background: #fbf8f2;
    }
    .two-col {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }
    .queue-card {
      padding: 14px 16px;
      border-radius: 16px;
      background: #faf5ec;
      border: 1px solid var(--line);
    }
    .queue-card + .queue-card {
      margin-top: 10px;
    }
    .agent-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
    }
    .agent-card {
      padding: 14px 16px;
      border-radius: 16px;
      background: #faf5ec;
      border: 1px solid var(--line);
    }
    .agent-focus {
      margin: 0 0 8px;
      color: #7c6340;
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .agent-card p {
      margin: 0;
    }
    .agent-card ul {
      margin-top: 10px;
    }
    .timeline-steps {
      display: grid;
      gap: 12px;
    }
    .timeline-step-card {
      display: grid;
      grid-template-columns: 52px 1fr;
      gap: 14px;
      padding: 14px 16px;
      border-radius: 16px;
      background: #faf5ec;
      border: 1px solid var(--line);
    }
    .timeline-step-pill {
      width: 52px;
      height: 52px;
      display: flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      background: #345f7d;
      color: #f6fbff;
      font-weight: 700;
    }
    .timeline-step-lane {
      margin: 0 0 6px;
      color: #7c6340;
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    @media print {
      body {
        padding: 0;
        background: #fff;
      }
      .panel {
        box-shadow: none;
      }
    }
  </style>
</head>
<body>
  <main class="sheet">
    <section class="panel">
      <div class="hero-grid">
        <div>
          <p class="eyebrow">txflow-risk executive briefing</p>
          <h1>${escapeHtml(manifest.title || "txflow-risk management brief")}</h1>
          <p class="muted">Current focus round: ${escapeHtml(currentRound?.round_name || manifest.meta?.round_name || "current")} | generated ${escapeHtml(generatedAt)} | view mode ${escapeHtml(state.viewMode)}</p>
          <ul>${summary.bullets.map((line) => `<li>${escapeHtml(line)}</li>`).join("")}</ul>
        </div>
        <div class="meta-grid">
          <div class="meta-card">
            <strong>current round</strong>
            <span>${escapeHtml(currentRound?.round_name || manifest.meta?.round_name || "-")}</span>
          </div>
          <div class="meta-card">
            <strong>previous round</strong>
            <span>${escapeHtml(previousRound?.round_name || "-")}</span>
          </div>
          <div class="meta-card">
            <strong>selected candidate</strong>
            <span>${escapeHtml(present(state.selectedSeller || "-", "account"))}</span>
          </div>
          <div class="meta-card">
            <strong>round count</strong>
            <span>${escapeHtml(roundRows.length)}</span>
          </div>
        </div>
      </div>
    </section>
    <section class="panel">
      <h2>Round Overview</h2>
      <table>
        <thead>
          <tr>
            <th>Round</th>
            <th>Strong Bridge</th>
            <th>Seller Candidates</th>
            <th>Bridge Rate</th>
            <th>Seller Recovery</th>
            <th>Val F1</th>
            <th>Top Candidate</th>
          </tr>
        </thead>
        <tbody>
          ${roundRows
            .map(
              (row) => `
                <tr>
                  <td>${escapeHtml(row.round_name)}</td>
                  <td>${escapeHtml(row.strong_bridge_candidates)}</td>
                  <td>${escapeHtml(row.returned_seller_candidates)}</td>
                  <td>${escapeHtml(row.bridge_candidate_rate.toFixed(4))}</td>
                  <td>${escapeHtml(row.seller_recovery_rate.toFixed(4))}</td>
                  <td>${escapeHtml(row.best_val_f1.toFixed(4))}</td>
                  <td>${escapeHtml(
                    row.top_candidate
                      ? `${row.top_candidate.seller_account} | buyers ${row.top_candidate.bridge_buyers} | uplift ${row.top_candidate.bridge_uplift.toFixed(4)}`
                      : "-"
                  )}</td>
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    </section>
    <section class="panel two-col">
      <div>
        <h2>Management Takeaways</h2>
        <ul>${summary.talkTrack.map((line) => `<li>${escapeHtml(line)}</li>`).join("")}</ul>
      </div>
      <div>
        <h2>Recommended Actions</h2>
        <ul>${(recommendations.length ? recommendations : ["Keep strong-bridge review as the first queue before expanding into weaker score-only candidates."])
          .map((line) => `<li>${escapeHtml(line)}</li>`)
          .join("")}</ul>
      </div>
    </section>
    <section class="panel">
      <h2>Agent Collaboration</h2>
      <div class="agent-grid">
        ${collaborationAgents.length
          ? collaborationAgents
              .map(
                (agent) => `
                  <article class="agent-card">
                    <h3>${escapeHtml(agent.name || "Agent")}</h3>
                    <p class="agent-focus">${escapeHtml(agent.focus || "-")}</p>
                    <p class="muted">${escapeHtml(agent.contribution || "-")}</p>
                    <ul>
                      ${Array.isArray(agent.evidence) ? agent.evidence.map((item) => `<li>${escapeHtml(item)}</li>`).join("") : ""}
                    </ul>
                    <p><strong>Adopted:</strong> ${escapeHtml(agent.adopted_change || "-")}</p>
                  </article>
                `
              )
              .join("")
          : "<p class='muted'>No collaboration summary available.</p>"}
      </div>
    </section>
    <section class="panel two-col">
      <div>
        <h2>Consensus And Conflicts</h2>
        <h3>Consensus</h3>
        <ul>${collaborationAgreements.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>
        <h3 style="margin-top:16px;">Conflicts</h3>
        <ul>${collaborationConflicts.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>
      </div>
      <div>
        <h2>Adopted Changes</h2>
        <ul>${collaborationAdoptedChanges.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>
      </div>
    </section>
    <section class="panel">
      <h2>Collaboration Timeline</h2>
      <div class="timeline-steps">
        ${collaborationTimeline
          .map(
            (item) => `
              <article class="timeline-step-card">
                <div class="timeline-step-pill">${escapeHtml(item.step || "-")}</div>
                <div>
                  <p class="timeline-step-lane">${escapeHtml(item.lane || "-")}</p>
                  <h3>${escapeHtml(item.title || "-")}</h3>
                  <p class="muted">${escapeHtml(item.detail || "-")}</p>
                </div>
              </article>
            `
          )
          .join("")}
      </div>
    </section>
    <section class="panel">
      <h2>Presentation Queue Snapshot</h2>
      ${topQueue.length
        ? topQueue
            .map(
              (item, index) => `
                <article class="queue-card">
                  <h3>${index + 1}. ${escapeHtml(present(item.seller_account, "account"))}</h3>
                  <p class="muted">${escapeHtml(`${item.candidate_tier || "unknown"} | bridge buyers ${item.bridge_buyers || 0} | uplift ${Number(item.bridge_uplift || 0).toFixed(4)}`)}</p>
                  <p>${escapeHtml(`support rows ${item.support_rows || 0} | unique buyers ${item.unique_buyers || 0} | review ${item.review_label || "unreviewed"}`)}</p>
                </article>
              `
            )
            .join("")
        : "<p class='muted'>No queue candidates available.</p>"}
    </section>
  </main>
</body>
</html>
`;
}

function buildBriefingMarkdown() {
  const manifest = state.manifest || {};
  const summary = executiveSummaryData();
  const { item, scriptLines } = presenterScriptLines();
  const collaboration = manifest.collaboration || {};
  const collaborationAgents = collaboration.agents || [];
  const collaborationAgreements = collaboration.agreements || [];
  const collaborationAdoptedChanges = collaboration.adopted_changes || [];
  const roundName = safeText(manifest.meta?.round_name || "current_round");
  const lines = [
    `# ${manifest.title || "txflow-risk showcase briefing"}`,
    "",
    `- round: ${roundName}`,
    `- view_mode: ${state.viewMode}`,
    `- selected_seller: ${item ? present(item.seller_account, "account") : "-"}`,
    "",
    "## Executive Summary",
    "",
    ...summary.bullets.map((line) => `- ${line}`),
    "",
    "## Talk Track",
    "",
    ...summary.talkTrack.map((line) => `- ${line}`),
    "",
    "## Presenter Notes",
    "",
    ...scriptLines.map((line) => `- ${line}`),
  ];
  if (item) {
    lines.push(
      "",
      "## Why This Candidate Was Elevated",
      "",
      `- Graph lane pushed this seller forward because it sits in ${item.candidate_tier || "unknown"} with bridge buyers ${item.bridge_buyers || 0} and uplift ${Number(item.bridge_uplift || 0).toFixed(4)}.`,
      "- Evaluation lane kept it credible by pairing the candidate story with frozen holdout recovery instead of raw row score alone.",
      "- Rules lane kept the training boundary stable, so this candidate priority is not explained away by holdout leakage."
    );
  }
  if (collaborationAgreements.length || collaborationAdoptedChanges.length) {
    lines.push("", "## Collaboration Snapshot", "");
    if (collaborationAgreements.length) {
      lines.push(...collaborationAgreements.slice(0, 3).map((line) => `- ${line}`));
    }
    if (collaborationAdoptedChanges.length) {
      lines.push(...collaborationAdoptedChanges.slice(0, 3).map((line) => `- ${line}`));
    }
  }
  if (collaborationAgents.length) {
    lines.push("", "## Agent Lanes", "");
    lines.push(
      ...collaborationAgents.map(
        (agent) =>
          `- ${agent.name || "Agent"}: ${agent.focus || "-"} | ${agent.contribution || "-"}`
      )
    );
  }
  if (item) {
    lines.push(
      "",
      "## Candidate Snapshot",
      "",
      `- seller_account: ${present(item.seller_account, "account")}`,
      `- candidate_tier: ${item.candidate_tier || "unknown"}`,
      `- bridge_buyers: ${item.bridge_buyers || 0}`,
      `- bridge_uplift: ${Number(item.bridge_uplift || 0).toFixed(4)}`,
      `- support_rows: ${item.support_rows || 0}`,
      `- unique_buyers: ${item.unique_buyers || 0}`,
      `- bridge_support_ratio: ${Number(item.bridge_support_ratio || 0).toFixed(2)}`,
      `- review_label: ${item.review_label || "unreviewed"}`
    );
  }
  return lines.join("\n") + "\n";
}

function buildBriefingHtml() {
  const manifest = state.manifest || {};
  const summary = executiveSummaryData();
  const { item, scriptLines } = presenterScriptLines();
  const roundName = safeText(manifest.meta?.round_name || "current_round");
  const title = safeText(manifest.title || "txflow-risk showcase briefing");
  const collaboration = manifest.collaboration || {};
  const collaborationAgents = collaboration.agents || [];
  const collaborationAgreements = collaboration.agreements || [];
  const collaborationAdoptedChanges = collaboration.adopted_changes || [];
  const snapshotRows = item
    ? [
        ["seller_account", present(item.seller_account, "account")],
        ["candidate_tier", item.candidate_tier || "unknown"],
        ["bridge_buyers", item.bridge_buyers || 0],
        ["bridge_uplift", Number(item.bridge_uplift || 0).toFixed(4)],
        ["support_rows", item.support_rows || 0],
        ["unique_buyers", item.unique_buyers || 0],
        ["bridge_support_ratio", Number(item.bridge_support_ratio || 0).toFixed(2)],
        ["review_label", item.review_label || "unreviewed"],
      ]
    : [];
  return `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${escapeHtml(title)}</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f1e7;
      --panel: #ffffff;
      --line: rgba(109, 85, 45, 0.14);
      --text: #1f2328;
      --muted: #66707a;
      --accent: #9f4e2f;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 32px;
      font-family: "Segoe UI", "PingFang SC", "Noto Sans SC", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(248, 205, 145, 0.24), transparent 28%),
        linear-gradient(180deg, #fcfbf7 0%, var(--bg) 100%);
    }
    .sheet {
      max-width: 980px;
      margin: 0 auto;
      display: grid;
      gap: 18px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 22px 24px;
      box-shadow: 0 14px 34px rgba(86, 63, 33, 0.08);
    }
    .eyebrow {
      margin: 0 0 8px;
      color: #7c6340;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 12px;
    }
    h1, h2 { margin: 0; letter-spacing: -0.03em; }
    h1 { font-size: 34px; }
    h2 { font-size: 22px; margin-bottom: 12px; }
    p, li { line-height: 1.65; }
    .meta {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-top: 16px;
    }
    .meta-card {
      padding: 14px 16px;
      border-radius: 16px;
      background: #faf5ec;
      border: 1px solid var(--line);
    }
    .meta-card strong {
      display: block;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #7c6340;
      margin-bottom: 8px;
    }
    ul {
      margin: 0;
      padding-left: 18px;
    }
    li + li {
      margin-top: 8px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }
    th, td {
      text-align: left;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }
    th {
      width: 220px;
      color: var(--muted);
      font-weight: 600;
    }
    .two-col {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }
    .agent-card {
      padding: 14px 16px;
      border-radius: 16px;
      background: #faf5ec;
      border: 1px solid var(--line);
    }
    .agent-card + .agent-card {
      margin-top: 10px;
    }
    .agent-card h3 {
      margin: 0 0 6px;
      font-size: 18px;
    }
    .agent-focus {
      margin: 0 0 8px;
      color: #7c6340;
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .agent-card p {
      margin: 0;
    }
    .muted {
      color: var(--muted);
    }
    @media print {
      body {
        padding: 0;
        background: #fff;
      }
      .panel {
        box-shadow: none;
        break-inside: avoid;
      }
    }
  </style>
</head>
<body>
  <main class="sheet">
    <section class="panel">
      <p class="eyebrow">txflow-risk briefing</p>
      <h1>${escapeHtml(title)}</h1>
      <p class="muted">Round ${escapeHtml(roundName)} | view mode ${escapeHtml(state.viewMode)} | generated from the showcase presenter flow.</p>
      <div class="meta">
        <div class="meta-card">
          <strong>selected seller</strong>
          <span>${escapeHtml(item ? present(item.seller_account, "account") : "-")}</span>
        </div>
        <div class="meta-card">
          <strong>candidate tier</strong>
          <span>${escapeHtml(item?.candidate_tier || "-")}</span>
        </div>
        <div class="meta-card">
          <strong>bridge buyers</strong>
          <span>${escapeHtml(item?.bridge_buyers || 0)}</span>
        </div>
      </div>
    </section>
    <section class="panel">
      <h2>Executive Summary</h2>
      <ul>${summary.bullets.map((line) => `<li>${escapeHtml(line)}</li>`).join("")}</ul>
    </section>
    <section class="panel">
      <h2>Talk Track</h2>
      <ul>${summary.talkTrack.map((line) => `<li>${escapeHtml(line)}</li>`).join("")}</ul>
    </section>
    <section class="panel">
      <h2>Presenter Notes</h2>
      <ul>${scriptLines.map((line) => `<li>${escapeHtml(line)}</li>`).join("")}</ul>
    </section>
    <section class="panel two-col">
      <div>
        <h2>Why This Candidate Was Elevated</h2>
        ${
          item
            ? `<ul>
              <li>${escapeHtml(`Graph lane pushed this seller forward because it sits in ${item.candidate_tier || "unknown"} with bridge buyers ${item.bridge_buyers || 0} and uplift ${Number(item.bridge_uplift || 0).toFixed(4)}.`)}</li>
              <li>${escapeHtml(`Evaluation lane kept it credible by pairing the candidate story with frozen holdout recovery instead of raw row score alone.`)}</li>
              <li>${escapeHtml(`Rules lane kept the training boundary stable, so this candidate priority is not explained away by holdout leakage.`)}</li>
            </ul>`
            : "<p class='muted'>No current candidate selected.</p>"
        }
      </div>
      <div>
        <h2>Collaboration Snapshot</h2>
        <ul>${collaborationAgreements.slice(0, 3).map((line) => `<li>${escapeHtml(line)}</li>`).join("")}</ul>
        <ul style="margin-top:12px;">${collaborationAdoptedChanges.slice(0, 3).map((line) => `<li>${escapeHtml(line)}</li>`).join("")}</ul>
      </div>
    </section>
    <section class="panel">
      <h2>Agent Lanes</h2>
      ${
        collaborationAgents.length
          ? collaborationAgents
              .map(
                (agent) => `
                  <article class="agent-card">
                    <h3>${escapeHtml(agent.name || "Agent")}</h3>
                    <p class="agent-focus">${escapeHtml(agent.focus || "-")}</p>
                    <p class="muted">${escapeHtml(agent.contribution || "-")}</p>
                  </article>
                `
              )
              .join("")
          : "<p class='muted'>No collaboration summary available.</p>"
      }
    </section>
    ${
      snapshotRows.length
        ? `<section class="panel">
      <h2>Candidate Snapshot</h2>
      <table>
        <tbody>
          ${snapshotRows
            .map(
              ([label, value]) => `
                <tr>
                  <th>${escapeHtml(label)}</th>
                  <td>${escapeHtml(value)}</td>
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    </section>`
        : ""
    }
  </main>
</body>
</html>
`;
}

function downloadBriefingMarkdown() {
  const manifest = state.manifest || {};
  const roundName = safeText(manifest.meta?.round_name || "round").replace(/[^a-zA-Z0-9_-]+/g, "_");
  const sellerName = safeText(state.selectedSeller || "candidate").replace(/[^a-zA-Z0-9@._-]+/g, "_");
  const filename = `${roundName}_${sellerName}_briefing.md`;
  const blob = new Blob([buildBriefingMarkdown()], { type: "text/markdown;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function downloadBriefingHtml() {
  const manifest = state.manifest || {};
  const roundName = safeText(manifest.meta?.round_name || "round").replace(/[^a-zA-Z0-9_-]+/g, "_");
  const sellerName = safeText(state.selectedSeller || "candidate").replace(/[^a-zA-Z0-9@._-]+/g, "_");
  const filename = `${roundName}_${sellerName}_briefing.html`;
  const blob = new Blob([buildBriefingHtml()], { type: "text/html;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function downloadExecutiveBriefingHtml() {
  const manifest = state.manifest || {};
  const roundName = safeText(manifest.meta?.round_name || "round").replace(/[^a-zA-Z0-9_-]+/g, "_");
  const filename = `${roundName}_executive_briefing.html`;
  const blob = new Blob([buildExecutiveBriefingHtml()], { type: "text/html;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function renderExecutiveControls() {
  const container = document.getElementById("executive-controls");
  if (!state.manifest) {
    container.innerHTML = "";
    return;
  }
  container.innerHTML = `
    <button class="chip" data-executive-action="export-html">Export Executive .html</button>
  `;
  container.querySelectorAll("[data-executive-action]").forEach((button) => {
    button.addEventListener("click", () => {
      if ((button.dataset.executiveAction || "") === "export-html") {
        downloadExecutiveBriefingHtml();
      }
    });
  });
}

function renderBriefingControls() {
  const container = document.getElementById("briefing-controls");
  if (!state.manifest) {
    container.innerHTML = "";
    return;
  }
  container.innerHTML = `
    <button class="chip" data-briefing-action="export-md">Export Briefing .md</button>
    <button class="chip" data-briefing-action="export-html">Export Briefing .html</button>
  `;
  container.querySelectorAll("[data-briefing-action]").forEach((button) => {
    button.addEventListener("click", () => {
      const action = button.dataset.briefingAction || "";
      if (action === "export-md") {
        downloadBriefingMarkdown();
      } else if (action === "export-html") {
        downloadBriefingHtml();
      }
    });
  });
}

function renderCards() {
  const container = document.getElementById("overview-cards");
  const cards = state.manifest?.overview_cards ?? [];
  container.innerHTML = cards
    .map(
      (item) => `
        <article class="metric-card">
          <div class="metric-label">${escapeHtml(item.label)}</div>
          <div class="metric-value">${escapeHtml(item.value)}</div>
          <div class="metric-note">${escapeHtml(item.note)}</div>
        </article>
      `
    )
    .join("");
}

function renderExecutiveSummary() {
  const container = document.getElementById("executive-summary");
  const manifest = state.manifest || {};
  const currentEntry = currentRoundEntry();
  const previousEntry = previousRoundEntry();
  const previousManifest = previousEntry?.manifest || null;
  const currentSummary = manifest.score_summary || {};
  const currentRecovery = manifest.seller_recovery || {};
  const currentMetrics = manifest.report_metrics || {};
  const previousSummary = previousManifest?.score_summary || {};
  const previousRecovery = previousManifest?.seller_recovery || {};
  const previousMetrics = previousManifest?.report_metrics || {};
  const currentStrong = Number(currentSummary.strong_bridge_candidates || 0);
  const previousStrong = Number(previousSummary.strong_bridge_candidates || 0);
  const currentRecoveryRate = Number(currentRecovery.recovery_rate || 0);
  const previousRecoveryRate = Number(previousRecovery.recovery_rate || 0);
  const currentValF1 = Number(currentMetrics.best_val_f1 || 0);
  const previousValF1 = Number(previousMetrics.best_val_f1 || 0);
  const topCandidate = manifest.seller_candidates?.[0] || null;
  const bullets = [
    `Current round exposes ${Number(currentSummary.returned_seller_candidates || manifest.seller_candidates?.length || 0)} seller candidates, with ${currentStrong} in the strong-bridge tier.`,
    `Frozen-eval seller recovery is ${currentRecoveryRate.toFixed(4)} and validation F1 is ${currentValF1.toFixed(4)}.`,
  ];
  if (previousManifest) {
    bullets.push(
      `Compared with ${previousEntry.round_name}, strong-bridge candidates changed by ${(currentStrong - previousStrong) >= 0 ? "+" : ""}${currentStrong - previousStrong}, seller recovery changed by ${(currentRecoveryRate - previousRecoveryRate).toFixed(4)}, and validation F1 changed by ${(currentValF1 - previousValF1).toFixed(4)}.`
    );
  }
  if (topCandidate) {
    bullets.push(
      `The first walkthrough candidate is ${present(topCandidate.seller_account, "account")}, supported by ${topCandidate.bridge_buyers} bridge buyers and uplift ${Number(topCandidate.bridge_uplift || 0).toFixed(4)}.`
    );
  }
  const topBridge = safeText(currentSummary.top_bridge_seller_candidate || "");
  container.innerHTML = `
    <div class="summary-grid">
      <section class="summary-block">
        <h3>Round Signal</h3>
        <p>${escapeHtml(`Round ${manifest.meta?.round_name || "-"}`)} is currently framed as a bridge-first seller extension pass. Presentation mode masks identities while keeping the extension logic readable.</p>
        <ul class="summary-list">
          ${bullets.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
        </ul>
      </section>
      <section class="summary-block">
        <h3>Talk Track</h3>
        <p>${escapeHtml(`Start with the strong-bridge queue, then explain how bridge buyers connect known seller seeds to the current unknown seller candidate set.`)}</p>
        <p>${escapeHtml(`Highest bridge anchor in this round: ${present(topBridge, "account")}. Use the Extension Story section to narrate one seller at a time.`)}</p>
      </section>
    </div>
  `;
}

function renderCollaboration() {
  const container = document.getElementById("agent-collaboration");
  const collaboration = state.manifest?.collaboration || {};
  const agents = collaboration.agents || [];
  const agreements = collaboration.agreements || [];
  const conflicts = collaboration.conflicts || [];
  const adoptedChanges = collaboration.adopted_changes || [];
  const timeline = collaboration.timeline || [];
  if (!agents.length && !agreements.length && !conflicts.length && !adoptedChanges.length && !timeline.length) {
    container.innerHTML = "<p class='muted'>No multi-agent collaboration summary attached to this round.</p>";
    return;
  }
  container.innerHTML = `
    <div class="collaboration-grid">
      <div class="agent-grid">
        ${agents
          .map(
            (agent) => `
              <article class="agent-card">
                <h3>${escapeHtml(agent.name || "Agent")}</h3>
                <p class="agent-focus">${escapeHtml(agent.focus || "-")}</p>
                <p>${escapeHtml(agent.contribution || "-")}</p>
                <ul class="agent-evidence">
                  ${Array.isArray(agent.evidence) ? agent.evidence.map((item) => `<li>${escapeHtml(item)}</li>`).join("") : ""}
                </ul>
                <div class="chip-row collaboration-links">
                  ${Array.isArray(agent.links)
                    ? agent.links
                        .map(
                          (link) => `
                            <button
                              class="chip"
                              data-collab-target="${escapeHtml(link.target || "")}"
                              data-collab-tier="${escapeHtml(link.tier || "")}"
                              data-collab-seller="${escapeHtml(link.seller || "")}"
                            >
                              ${escapeHtml(link.label || "Open")}
                            </button>
                          `
                        )
                        .join("")
                    : ""}
                </div>
                <p><strong>Adopted:</strong> ${escapeHtml(agent.adopted_change || "-")}</p>
              </article>
            `
          )
          .join("")}
      </div>
      <div class="collaboration-side">
        <section class="collaboration-box">
          <h3>Consensus</h3>
          <ul>
            ${agreements.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
          </ul>
        </section>
        <section class="collaboration-box">
          <h3>Conflicts</h3>
          <ul>
            ${conflicts.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
          </ul>
        </section>
        <section class="collaboration-box">
          <h3>Adopted Changes</h3>
          <ul>
            ${adoptedChanges.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
          </ul>
        </section>
      </div>
    </div>
    <div class="collaboration-timeline">
      <h3>Collaboration Timeline</h3>
      <div class="timeline-steps">
        ${timeline
          .map(
            (item) => `
              <article class="timeline-step-card">
                <div class="timeline-step-pill">${escapeHtml(item.step || "-")}</div>
                <div>
                  <p class="timeline-step-lane">${escapeHtml(item.lane || "-")}</p>
                  <h4>${escapeHtml(item.title || "-")}</h4>
                  <p>${escapeHtml(item.detail || "-")}</p>
                  <div class="chip-row collaboration-links">
                    ${Array.isArray(item.links)
                      ? item.links
                          .map(
                            (link) => `
                              <button
                                class="chip"
                                data-collab-target="${escapeHtml(link.target || "")}"
                                data-collab-tier="${escapeHtml(link.tier || "")}"
                                data-collab-seller="${escapeHtml(link.seller || "")}"
                              >
                                ${escapeHtml(link.label || "Open")}
                              </button>
                            `
                          )
                          .join("")
                      : ""}
                  </div>
                </div>
              </article>
            `
          )
          .join("")}
      </div>
    </div>
  `;
  container.querySelectorAll("[data-collab-target]").forEach((button) => {
    button.addEventListener("click", () => {
      const target = button.dataset.collabTarget || "";
      const tier = button.dataset.collabTier || "";
      const seller = button.dataset.collabSeller || "";
      handleCollaborationJump(target, tier, seller);
    });
  });
}

function renderPresentationQueue() {
  const container = document.getElementById("presentation-queue");
  const items = queueCandidates();
  if (!items.length) {
    container.innerHTML = "<p class='muted'>No candidates available for presentation.</p>";
    return;
  }
  container.innerHTML = `
    <div class="queue-list">
      ${items
        .map(
          (item, index) => `
            <article class="queue-card ${state.selectedSeller === item.seller_account ? "selected" : ""}" data-queue-index="${index}">
              <h3>${index + 1}. ${escapeHtml(present(item.seller_account, "account"))}</h3>
              <p>${escapeHtml(
                `${item.bridge_buyers} bridge buyers | uplift ${Number(item.bridge_uplift || 0).toFixed(4)} | tier ${item.candidate_tier || "unknown"}`
              )}</p>
              <p>${escapeHtml(
                `Support rows ${item.support_rows} | unique buyers ${item.unique_buyers} | review ${item.review_label || "unreviewed"}`
              )}</p>
            </article>
          `
        )
        .join("")}
    </div>
  `;
  container.querySelectorAll("[data-queue-index]").forEach((card) => {
    card.addEventListener("click", () => {
      stopAutoplay();
      selectPresentationIndex(Number(card.dataset.queueIndex || 0));
      render();
    });
  });
}

function renderPresenterNotes() {
  const container = document.getElementById("presenter-notes");
  const manifest = state.manifest || {};
  const item = (manifest.seller_candidates || []).find((entry) => entry.seller_account === state.selectedSeller);
  if (!item) {
    container.innerHTML = "<p class='muted'>Select a candidate to generate speaking notes.</p>";
    return;
  }
  const previousEntry = previousRoundEntry();
  const previousManifest = previousEntry?.manifest || null;
  const previousMatch = previousManifest?.seller_candidates?.find((entry) => entry.seller_account === item.seller_account) || null;
  const upliftDelta = previousMatch ? Number(item.bridge_uplift || 0) - Number(previousMatch.bridge_uplift || 0) : null;
  const buyerDelta = previousMatch ? Number(item.bridge_buyers || 0) - Number(previousMatch.bridge_buyers || 0) : null;
  const topBuyers = (item.buyer_support || [])
    .slice(0, 2)
    .map((row) => present(row.buyer_account, "account"))
    .join("、") || "暂无";
  const scriptLines = [
    `这一页我重点讲候选 ${present(item.seller_account, "account")}。`,
    `它当前位于 ${item.candidate_tier || "unknown"} 层，bridge buyers 数量是 ${item.bridge_buyers || 0}，bridge uplift 是 ${Number(item.bridge_uplift || 0).toFixed(4)}。`,
    `支撑它的关键买方主要包括 ${topBuyers}，这些买方之前已经和已知 seller 种子发生过联系，所以它不是孤立高分，而是有扩线桥接证据。`,
    `从交易面上看，这个候选目前有 ${item.support_rows || 0} 条支持交易，覆盖 ${item.unique_buyers || 0} 个买方，桥接比例 ${Number(item.bridge_support_ratio || 0).toFixed(2)}。`,
  ];
  if (previousMatch) {
    scriptLines.push(
      `和上一轮 ${previousEntry.round_name} 相比，它的 bridge uplift 变化 ${(upliftDelta >= 0 ? "+" : "") + upliftDelta.toFixed(4)}，bridge buyers 变化 ${(buyerDelta >= 0 ? "+" : "") + buyerDelta}。`
    );
  } else if (previousEntry) {
    scriptLines.push(`上一轮 ${previousEntry.round_name} 里，这个候选没有进入当前展示队列，所以它可以视为这一轮更值得讲的新优先对象。`);
  }
  scriptLines.push("汇报时建议先讲 bridge 证据，再讲支持交易数量，最后落到人工复核优先级。");
  container.innerHTML = `
    <article class="notes-card">
      <h3>${escapeHtml(present(item.seller_account, "account"))}</h3>
      <div class="chip-row">
        <span class="chip">${escapeHtml(item.candidate_tier || "unknown")}</span>
        <span class="chip">bridge buyers ${escapeHtml(item.bridge_buyers || 0)}</span>
        <span class="chip">uplift ${Number(item.bridge_uplift || 0).toFixed(4)}</span>
      </div>
      <p class="notes-script">${escapeHtml(scriptLines.join("\n\n"))}</p>
    </article>
  `;
}

function renderRecommendations() {
  const container = document.getElementById("recommendations");
  const items = state.manifest?.recommendations ?? [];
  container.innerHTML = items.map((item) => `<li>${escapeHtml(item)}</li>`).join("") || "<li>No recommendations.</li>";
}

function renderComparison() {
  const container = document.getElementById("comparison-table");
  const rows = state.manifest?.comparison_rounds ?? [];
  if (!rows.length) {
    container.innerHTML = "<p class='muted'>No comparison data attached.</p>";
    return;
  }
  container.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Round</th>
          <th>Val F1</th>
          <th>Train Nodes</th>
          <th>Val Nodes</th>
          <th>Review +</th>
          <th>Review -</th>
        </tr>
      </thead>
      <tbody>
        ${rows
          .map(
            (item) => `
              <tr>
                <td>${escapeHtml(item.round_name)}</td>
                <td>${Number(item.best_val_f1 || 0).toFixed(4)}</td>
                <td>${escapeHtml(item.train_nodes)}</td>
                <td>${escapeHtml(item.val_nodes)}</td>
                <td>${escapeHtml(item.confirmed_positive)}</td>
                <td>${escapeHtml(item.confirmed_negative)}</td>
              </tr>
            `
          )
          .join("")}
      </tbody>
    </table>
  `;
}

function renderTierChips() {
  const container = document.getElementById("tier-chips");
  container.innerHTML = Object.entries(tierLabels)
    .map(
      ([key, label]) => `
        <button class="chip ${state.tier === key ? "active" : ""}" data-tier="${key}">
          ${escapeHtml(label)}
        </button>
      `
    )
    .join("");
  container.querySelectorAll("[data-tier]").forEach((button) => {
    button.addEventListener("click", () => {
      state.tier = button.dataset.tier || "all";
      if (!currentCandidates().some((item) => item.seller_account === state.selectedSeller)) {
        state.selectedSeller = currentCandidates()[0]?.seller_account || "";
      }
      syncPresentationIndex();
      render();
    });
  });
}

function renderSellerTable() {
  const container = document.getElementById("seller-table");
  const rows = currentCandidates();
  if (!rows.length) {
    container.innerHTML = "<p class='muted'>No seller candidates match the current filters.</p>";
    return;
  }
  if (!state.selectedSeller) {
    state.selectedSeller = rows[0].seller_account;
  }
  container.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Seller Account</th>
          <th>Tier</th>
          <th>Score</th>
          <th>Uplift</th>
          <th>Bridge Buyers</th>
          <th>Bridge Ratio</th>
          <th>Known Links</th>
          <th>Review</th>
        </tr>
      </thead>
      <tbody>
        ${rows
          .map(
            (item) => `
              <tr class="${state.selectedSeller === item.seller_account ? "selected" : ""}" data-seller="${escapeHtml(item.seller_account)}">
                <td>${escapeHtml(present(item.seller_account, "account"))}</td>
                <td>${escapeHtml(item.candidate_tier || "-")}</td>
                <td>${Number(item.score || 0).toFixed(4)}</td>
                <td>${Number(item.bridge_uplift || 0).toFixed(4)}</td>
                <td>${escapeHtml(item.bridge_buyers)}</td>
                <td>${Number(item.bridge_support_ratio || 0).toFixed(2)}</td>
                <td>${escapeHtml(item.known_buyer_support)}</td>
                <td>${badge(item.review_label)}</td>
              </tr>
            `
          )
          .join("")}
      </tbody>
    </table>
  `;
  container.querySelectorAll("[data-seller]").forEach((row) => {
    row.addEventListener("click", () => {
      stopAutoplay();
      state.selectedSeller = row.dataset.seller || "";
      syncPresentationIndex();
      renderSellerTable();
      renderBridgeGraph();
      renderStoryPanel();
      renderCandidateDetail();
      renderPresentationControls();
      renderPresentationQueue();
    });
  });
}

function renderBridgeGraph() {
  const container = document.getElementById("bridge-graph");
  const selected = state.selectedSeller;
  const edges = (state.manifest?.bridge_graph?.edges || []).filter((item) => !selected || item.seller === selected);
  if (!edges.length) {
    container.innerHTML = "<p class='muted'>No bridge edges for the selected seller.</p>";
    return;
  }
  container.innerHTML = edges
    .map(
      (item) => `
        <div class="bridge-edge">
          <strong>${escapeHtml(present(item.buyer, "account"))}</strong> → <strong>${escapeHtml(present(item.seller, "account"))}</strong><br />
          rows ${escapeHtml(item.rows)} | max_score ${Number(item.max_score || 0).toFixed(4)}
        </div>
      `
    )
    .join("");
}

function renderFrozenEval() {
  const container = document.getElementById("frozen-eval");
  const manifest = state.manifest || {};
  const metrics = manifest.frozen_eval_metrics || {};
  const recovery = manifest.seller_recovery || {};
  const rows = [
    ["F1", Number(metrics.f1 || 0).toFixed(4)],
    ["Precision", Number(metrics.precision || 0).toFixed(4)],
    ["Recall", Number(metrics.recall || 0).toFixed(4)],
    ["Positive sellers", safeText(recovery.holdout_positive_seller_count || 0)],
    ["Recovered sellers", safeText(recovery.recovered_positive_sellers || 0)],
    ["Recovery rate", Number(recovery.recovery_rate || 0).toFixed(4)],
  ];
  container.innerHTML = rows
    .map(
      ([label, value]) => `
        <div class="key-value-row">
          <span>${escapeHtml(label)}</span>
          <strong>${escapeHtml(value)}</strong>
        </div>
      `
    )
    .join("");
}

function renderStoryPanel() {
  const container = document.getElementById("story-panel");
  const manifest = state.manifest || {};
  const item = (manifest.seller_candidates || []).find((entry) => entry.seller_account === state.selectedSeller);
  if (!item) {
    container.innerHTML = "<p class='muted'>Select a seller candidate to generate the extension story.</p>";
    return;
  }
  const scoreSummary = manifest.score_summary || {};
  const buyerRows = (item.buyer_support || []).slice(0, 3);
  const topBuyers = buyerRows.map((row) => present(row.buyer_account, "account")).join("、") || "暂无";
  const sampleNames = (item.sample_counterparties || []).map((value) => present(value, "name")).join("、") || "暂无";
  const topTime = safeText(item.support_examples?.[0]?.timestamp || "-");
  const steps = [
    {
      title: "已知 seller 种子提供起点",
      body: `当前轮次先从 ${safeText(scoreSummary.known_seller_seeds || 0)} 个已知 seller 种子出发，其中推断出来的 anchor sellers 有 ${safeText(scoreSummary.inferred_anchor_sellers || 0)} 个。`,
    },
    {
      title: "bridge buyers 把已知种子和当前候选连起来",
      body: `候选 ${present(item.seller_account, "account")} 由 ${safeText(item.bridge_buyers || 0)} 个 bridge buyers 支撑，累计 known seller links 为 ${safeText(item.known_buyer_support || 0)}。优先支撑的 buyers 包括：${topBuyers}。`,
    },
    {
      title: "候选 seller 在多笔交易里重复出现",
      body: `该候选目前有 ${safeText(item.support_rows || 0)} 条支持交易，覆盖 ${safeText(item.unique_buyers || 0)} 个买方和 ${safeText(item.unique_workbooks || 0)} 个账本；桥接比例 ${Number(item.bridge_support_ratio || 0).toFixed(2)}，bridge uplift ${Number(item.bridge_uplift || 0).toFixed(4)}。`,
    },
    {
      title: "人工复核时先看什么",
      body: `先核对 ${present(item.seller_account, "account")} 在 ${topTime} 附近的支持交易，再核对样本对手方 ${sampleNames}，最后确认它是否应保留在 ${safeText(item.candidate_tier || "unknown")} 层。`,
    },
  ];
  container.innerHTML = `
    <div class="story-shell">
      <div class="story-summary">
        <span class="chip">${escapeHtml(item.candidate_tier || "unknown")}</span>
        <span class="chip">bridge buyers ${escapeHtml(item.bridge_buyers || 0)}</span>
        <span class="chip">known links ${escapeHtml(item.known_buyer_support || 0)}</span>
        <span class="chip">support rows ${escapeHtml(item.support_rows || 0)}</span>
      </div>
      <div class="story-steps">
        ${steps
          .map(
            (step, index) => `
              <article class="story-step">
                <div class="story-step-index">${index + 1}</div>
                <div>
                  <h4>${escapeHtml(step.title)}</h4>
                  <p>${escapeHtml(step.body)}</p>
                </div>
              </article>
            `
          )
          .join("")}
      </div>
    </div>
  `;
}

function renderTimeline(supportRows) {
  if (!supportRows.length) {
    return "<p class='muted'>No support timeline.</p>";
  }
  const maxScore = Math.max(...supportRows.map((item) => Number(item.score || 0)), 0.01);
  return `
    <div class="timeline">
      ${supportRows
        .slice(0, 8)
        .map((row) => {
          const height = 42 + Math.round((Number(row.score || 0) / maxScore) * 108);
          const label = safeText(row.timestamp || row.row_index || "").slice(0, 16);
          return `
            <div class="timeline-bar" style="height:${height}px" title="${escapeHtml(
              `${safeText(row.timestamp)} | ${present(row.buyer_account, "account")} | ${safeText(row.amount)}`
            )}">
              <div class="timeline-bar-label">${escapeHtml(label || "-")}</div>
            </div>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderCandidateDetail() {
  const container = document.getElementById("candidate-detail");
  const empty = document.getElementById("detail-empty");
  const item = (state.manifest?.seller_candidates || []).find((entry) => entry.seller_account === state.selectedSeller);
  if (!item) {
    empty.style.display = "";
    container.innerHTML = "";
    return;
  }
  empty.style.display = "none";
  const buyerRows = item.buyer_support || [];
  const supportRows = item.support_examples || [];
  container.innerHTML = `
    <div class="detail-shell">
      <div class="detail-head">
        <div>
          <h3>${escapeHtml(present(item.seller_account, "account"))}</h3>
          <p class="detail-subtitle">${escapeHtml((item.sample_counterparties || []).map((value) => present(value, "name")).join(" | ") || "No counterparty samples")}</p>
        </div>
        <div class="chip-row">
          <span class="chip">${escapeHtml(item.candidate_tier || "unknown")}</span>
          <span class="chip">score ${Number(item.score || 0).toFixed(4)}</span>
          <span class="chip">uplift ${Number(item.bridge_uplift || 0).toFixed(4)}</span>
          <span class="chip">bridge buyers ${safeText(item.bridge_buyers || 0)}</span>
          ${badge(item.review_label)}
        </div>
      </div>
      <div class="detail-grid">
        <div class="detail-panel">
          <h4>Support Timeline</h4>
          ${renderTimeline(supportRows)}
        </div>
        <div class="detail-panel">
          <h4>Review State</h4>
          <div class="key-value-list">
            <div class="key-value-row"><span>Review</span><strong>${escapeHtml(item.review_label || "unreviewed")}</strong></div>
            <div class="key-value-row"><span>Note</span><strong>${escapeHtml(present(item.review_note || "-", "note"))}</strong></div>
            <div class="key-value-row"><span>Unique buyers</span><strong>${escapeHtml(item.unique_buyers || 0)}</strong></div>
            <div class="key-value-row"><span>Support rows</span><strong>${escapeHtml(item.support_rows || 0)}</strong></div>
          </div>
        </div>
      </div>
      <div class="detail-grid">
        <div class="detail-panel">
          <h4>Buyer Support</h4>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Buyer</th>
                  <th>Bridge</th>
                  <th>Known Links</th>
                  <th>Rows</th>
                  <th>Max Score</th>
                </tr>
              </thead>
              <tbody>
                ${buyerRows
                  .map(
                    (row) => `
                      <tr>
                        <td>${escapeHtml(present(row.buyer_account, "account"))}</td>
                        <td>${row.bridge_buyer ? "yes" : "-"}</td>
                        <td>${escapeHtml(row.known_seller_links)}</td>
                        <td>${escapeHtml(row.rows)}</td>
                        <td>${Number(row.max_score || 0).toFixed(4)}</td>
                      </tr>
                    `
                  )
                  .join("")}
              </tbody>
            </table>
          </div>
        </div>
        <div class="detail-panel">
          <h4>Supporting Rows</h4>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Score</th>
                  <th>Bridge</th>
                  <th>Buyer</th>
                  <th>Amount</th>
                  <th>Time</th>
                  <th>Counterparty</th>
                </tr>
              </thead>
              <tbody>
                ${supportRows
                  .map(
                    (row) => `
                      <tr>
                        <td>${Number(row.score || 0).toFixed(4)}</td>
                        <td>${row.bridge_buyer ? "yes" : "-"}</td>
                        <td>${escapeHtml(present(row.buyer_account || "-", "account"))}</td>
                        <td>${escapeHtml(row.amount || "")}</td>
                        <td>${escapeHtml(row.timestamp || "")}</td>
                        <td>${escapeHtml(present(row.counterparty_name || row.counterparty || "-", "name"))}</td>
                      </tr>
                    `
                  )
                  .join("")}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  `;
}

function render() {
  document.body.classList.toggle("presentation-layout", state.presentationLayout);
  document.getElementById("page-title").textContent = state.manifest?.title || "txflow-risk showcase";
  document.getElementById("page-subtitle").textContent =
    state.viewMode === "internal"
      ? `Round ${state.manifest?.meta?.round_name || "-"} | score source ${state.manifest?.meta?.score_source || "-"}`
      : `Round ${state.manifest?.meta?.round_name || "-"} | presentation mode with masked sensitive fields`;
  document.getElementById("manifest-path").textContent =
    state.viewMode === "internal"
      ? (state.manifest?.meta?.score_source || DEFAULT_MANIFEST)
      : shortPath(state.manifest?.meta?.score_source || DEFAULT_MANIFEST);
  renderRoundChips();
  renderModeChips();
  renderExecutiveControls();
  renderBriefingControls();
  syncPresentationIndex();
  renderPresentationControls();
  renderExecutiveSummary();
  renderCollaboration();
  renderPresentationQueue();
  renderPresenterNotes();
  renderCards();
  renderRecommendations();
  renderComparison();
  renderTierChips();
  renderSellerTable();
  renderBridgeGraph();
  renderFrozenEval();
  renderStoryPanel();
  renderCandidateDetail();
}

async function loadManifest() {
  const params = new URLSearchParams(window.location.search);
  const manifestPath = params.get("manifest") || DEFAULT_MANIFEST;
  const initialView = params.get("view");
  const requestedRound = params.get("round") || "";
  if (initialView === "public" || initialView === "internal") {
    state.viewMode = initialView;
  }
  if (window.__TXFLOW_SHOWCASE_INDEX__?.rounds?.length) {
    state.showcaseIndex = window.__TXFLOW_SHOWCASE_INDEX__;
    const initialEntry =
      state.showcaseIndex.rounds.find((item) => item.round_name === requestedRound) || state.showcaseIndex.rounds[0];
    state.roundName = initialEntry?.round_name || "";
    state.manifest = initialEntry?.manifest || null;
    if (state.manifest) {
      if (!initialView) {
        state.viewMode = state.manifest?.meta?.presentation_mode_default || "public";
      }
      state.selectedSeller = state.manifest?.seller_candidates?.[0]?.seller_account || "";
      stopAutoplay();
      render();
      return;
    }
  }
  if (window.__TXFLOW_SHOWCASE__) {
    state.manifest = window.__TXFLOW_SHOWCASE__;
    state.roundName = state.manifest?.meta?.round_name || "";
    state.showcaseIndex = {
      rounds: [
        {
          round_name: state.roundName || "current",
          title: state.manifest?.title || state.roundName || "current",
          manifest: state.manifest,
        },
      ],
    };
    state.viewMode = state.viewMode || state.manifest?.meta?.presentation_mode_default || "public";
    state.selectedSeller = state.manifest?.seller_candidates?.[0]?.seller_account || "";
    stopAutoplay();
    render();
    return;
  }
  const response = await fetch(manifestPath);
  if (!response.ok) {
    throw new Error(`failed to load manifest: ${response.status}`);
  }
  state.manifest = await response.json();
  state.roundName = state.manifest?.meta?.round_name || "";
  state.showcaseIndex = {
    rounds: [
      {
        round_name: state.roundName || "current",
        title: state.manifest?.title || state.roundName || "current",
        manifest: state.manifest,
      },
    ],
  };
  if (!initialView) {
    state.viewMode = state.manifest?.meta?.presentation_mode_default || "public";
  }
  state.selectedSeller = state.manifest?.seller_candidates?.[0]?.seller_account || "";
  stopAutoplay();
  render();
}

document.addEventListener("fullscreenchange", () => {
  if (!document.fullscreenElement && state.presentationLayout) {
    state.presentationLayout = false;
    render();
  }
});

document.addEventListener("keydown", async (event) => {
  if (!state.manifest) {
    return;
  }
  if (event.key === "ArrowLeft") {
    stopAutoplay();
    selectPresentationIndex(state.presentationIndex - 1);
    render();
  } else if (event.key === "ArrowRight") {
    stopAutoplay();
    selectPresentationIndex(state.presentationIndex + 1);
    render();
  } else if (event.key === " " || event.code === "Space") {
    event.preventDefault();
    if (state.autoplayTimer) {
      stopAutoplay();
    } else {
      state.autoplayTimer = window.setInterval(() => {
        selectPresentationIndex(state.presentationIndex + 1);
        render();
      }, 3500);
    }
    render();
  } else if (event.key.toLowerCase() === "f") {
    await setPresentationLayout(!state.presentationLayout);
    render();
  } else if (event.key === "Escape" && state.autoplayTimer) {
    stopAutoplay();
    render();
  }
});

document.getElementById("seller-filter").addEventListener("input", (event) => {
  state.query = event.target.value || "";
  if (!currentCandidates().some((item) => item.seller_account === state.selectedSeller)) {
    state.selectedSeller = currentCandidates()[0]?.seller_account || "";
  }
  renderSellerTable();
  renderBridgeGraph();
  renderCandidateDetail();
});

loadManifest().catch((error) => {
  document.getElementById("page-title").textContent = "Failed to load showcase";
  document.getElementById("page-subtitle").textContent = error.message;
});
