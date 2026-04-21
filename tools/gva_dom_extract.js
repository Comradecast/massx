(async () => {
  const CANONICAL_FIELDS = [
    "incident_id",
    "incident_date",
    "state",
    "city_or_county",
    "address",
    "victims_killed",
    "victims_injured",
    "suspects_killed",
    "suspects_injured",
    "suspects_arrested",
    "incident_url",
    "source_url",
  ];

  const HEADER_ALIASES = {
    incident_id: ["incident id", "id"],
    incident_date: ["incident date", "date"],
    state: ["state"],
    city_or_county: ["city or county", "city/county", "city", "county"],
    address: ["address"],
    victims_killed: ["victims killed", "killed"],
    victims_injured: ["victims injured", "injured"],
    suspects_killed: ["suspects killed"],
    suspects_injured: ["suspects injured"],
    suspects_arrested: ["suspects arrested", "arrested"],
    operations: ["operations"],
  };

  const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
  const normalizeKey = (value) => normalize(value).toLowerCase();
  const isVisible = (element) => !!(element && (element.offsetParent || element.getClientRects().length));

  const findCanonicalField = (headerText) => {
    const normalizedHeader = normalizeKey(headerText);
    for (const [field, aliases] of Object.entries(HEADER_ALIASES)) {
      if (aliases.includes(normalizedHeader)) {
        return field;
      }
    }
    return null;
  };

  const scoreTable = (table) => {
    const headers = Array.from(table.querySelectorAll("thead th, tr th"))
      .map((node) => normalize(node.textContent))
      .filter(Boolean);
    const fields = headers.map(findCanonicalField).filter(Boolean);
    return {
      table,
      headers,
      recognizedCount: new Set(fields).size,
    };
  };

  const candidateTables = Array.from(document.querySelectorAll("table"))
    .filter(isVisible)
    .map(scoreTable)
    .sort((left, right) => right.recognizedCount - left.recognizedCount);

  if (!candidateTables.length || candidateTables[0].recognizedCount < 5) {
    throw new Error("No visible GVA-style table with recognizable headers was found on the page.");
  }

  const table = candidateTables[0].table;
  const headerCells = Array.from(table.querySelectorAll("thead tr th"));
  const fallbackHeaderCells = headerCells.length ? headerCells : Array.from(table.querySelectorAll("tr th"));
  const headerMap = {};

  fallbackHeaderCells.forEach((headerCell, index) => {
    const field = findCanonicalField(headerCell.textContent);
    if (field) {
      headerMap[field] = index;
    }
  });

  const bodyRows = Array.from(table.querySelectorAll("tbody tr")).filter(isVisible);
  const rows = bodyRows.length ? bodyRows : Array.from(table.querySelectorAll("tr")).filter((row) => row.querySelectorAll("td").length > 0);

  if (!rows.length) {
    throw new Error("A candidate table was found, but no data rows were visible.");
  }

  const getCellText = (cells, field) => {
    const index = headerMap[field];
    if (index === undefined || !cells[index]) {
      return "";
    }
    return normalize(cells[index].textContent);
  };

  const extractLinks = (row) => {
    const links = Array.from(row.querySelectorAll("a[href]"))
      .map((anchor) => ({
        text: normalizeKey(anchor.textContent),
        href: normalize(anchor.href),
      }))
      .filter((link) => link.href.startsWith("http"));

    let incidentUrl = "";
    let sourceUrl = "";

    for (const link of links) {
      if (!incidentUrl && (link.text.includes("view incident") || link.text.includes("incident"))) {
        incidentUrl = link.href;
        continue;
      }
      if (!sourceUrl && (link.text.includes("view source") || link.text.includes("source") || link.text.includes("article"))) {
        sourceUrl = link.href;
      }
    }

    for (const link of links) {
      if (!incidentUrl && link.href.includes("gunviolencearchive.org")) {
        incidentUrl = link.href;
        continue;
      }
      if (!sourceUrl && !link.href.includes("gunviolencearchive.org")) {
        sourceUrl = link.href;
      }
    }

    return { incidentUrl, sourceUrl };
  };

  const records = rows
    .map((row) => {
      const cells = Array.from(row.children).filter((cell) => ["TD", "TH"].includes(cell.tagName));
      if (!cells.length) {
        return null;
      }

      const links = extractLinks(row);
      const record = {
        incident_id: getCellText(cells, "incident_id"),
        incident_date: getCellText(cells, "incident_date"),
        state: getCellText(cells, "state"),
        city_or_county: getCellText(cells, "city_or_county"),
        address: getCellText(cells, "address"),
        victims_killed: getCellText(cells, "victims_killed"),
        victims_injured: getCellText(cells, "victims_injured"),
        suspects_killed: getCellText(cells, "suspects_killed"),
        suspects_injured: getCellText(cells, "suspects_injured"),
        suspects_arrested: getCellText(cells, "suspects_arrested"),
        incident_url: links.incidentUrl,
        source_url: links.sourceUrl,
      };

      if (!record.incident_id) {
        return null;
      }
      return record;
    })
    .filter(Boolean);

  if (!records.length) {
    throw new Error("No extractable incident rows were found in the visible table.");
  }

  const missingUrlCount = records.filter((record) => !record.incident_url || !record.source_url).length;
  const jsonText = JSON.stringify(records, null, 2);

  if (typeof copy === "function") {
    copy(jsonText);
  } else if (navigator.clipboard && navigator.clipboard.writeText) {
    await navigator.clipboard.writeText(jsonText);
  } else {
    console.warn("Clipboard copy is not available in this context. The JSON payload is printed below.");
  }

  console.table(records.slice(0, 10), CANONICAL_FIELDS);
  console.log(`Extracted ${records.length} rows.`);
  if (missingUrlCount) {
    console.warn(`${missingUrlCount} row(s) are missing incident_url or source_url. Review before conversion.`);
  }
  if (typeof copy !== "function" && !(navigator.clipboard && navigator.clipboard.writeText)) {
    console.log(jsonText);
  }
})();
