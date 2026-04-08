export default async function handler(req, res) {
  try {
    const site = "01618100";

    const today = new Date();
    const endDate = formatDate(today);
    const startDate = formatDate(new Date(today.getFullYear() - 20, today.getMonth(), today.getDate()));

    const ivUrl =
      `https://waterservices.usgs.gov/nwis/iv/?format=json&sites=${site}&parameterCd=00060,00065`;

    const dvUrl =
      `https://waterservices.usgs.gov/nwis/dv/?format=json&sites=${site}&parameterCd=00060` +
      `&statCd=00003&startDT=${startDate}&endDT=${endDate}`;

    const [ivRes, dvRes] = await Promise.all([fetch(ivUrl), fetch(dvUrl)]);

    if (!ivRes.ok) {
      throw new Error(`IV request failed: ${ivRes.status}`);
    }
    if (!dvRes.ok) {
      throw new Error(`DV request failed: ${dvRes.status}`);
    }

    const ivJson = await ivRes.json();
    const dvJson = await dvRes.json();

    const flowSeries = ivJson?.value?.timeSeries?.find(
      (s) => s?.variable?.variableCode?.[0]?.value === "00060"
    );
    const stageSeries = ivJson?.value?.timeSeries?.find(
      (s) => s?.variable?.variableCode?.[0]?.value === "00065"
    );

    const currentFlowPoint = flowSeries?.values?.[0]?.value?.slice(-1)?.[0];
    const currentStagePoint = stageSeries?.values?.[0]?.value?.slice(-1)?.[0];

    const currentFlow = Number(currentFlowPoint?.value);
    const currentStage = Number(currentStagePoint?.value);
    const asOf = currentFlowPoint?.dateTime || null;

    const dailySeries = dvJson?.value?.timeSeries?.find(
      (s) => s?.variable?.variableCode?.[0]?.value === "00060"
    );

    const dailyValuesRaw = dailySeries?.values?.[0]?.value || [];

    const seasonalPool = buildSeasonalPool(dailyValuesRaw, today, 30);
    const seasonalValues = seasonalPool.map((x) => x.value).filter(Number.isFinite);

    const p25 = percentile(seasonalValues, 0.25);
    const p75 = percentile(seasonalValues, 0.75);
    const p90 = percentile(seasonalValues, 0.90);

    const seasonalLabel = classifySeasonalFlow(currentFlow, p25, p75, p90);
    const narrative = buildNarrative(seasonalLabel);

    const years = new Set(
      seasonalPool
        .map((x) => x.date.getFullYear())
        .filter(Number.isFinite)
    );

    res.setHeader("Access-Control-Allow-Origin", "*");
    res.status(200).json({
      site: `USGS-${site}`,
      flow: {
        currentCfs: Number.isFinite(currentFlow) ? round(currentFlow, 2) : null,
        gageHeightFt: Number.isFinite(currentStage) ? round(currentStage, 2) : null,
        asOf,
        seasonal: {
          windowDays: 30,
          recordYears: years.size,
          sampleSize: seasonalValues.length,
          p25: Number.isFinite(p25) ? round(p25, 2) : null,
          p75: Number.isFinite(p75) ? round(p75, 2) : null,
          p90: Number.isFinite(p90) ? round(p90, 2) : null,
          label: seasonalLabel,
          narrative
        }
      }
    });
  } catch (err) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.status(500).json({
      error: err.message || "Unknown error"
    });
  }
}

function formatDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function round(n, digits = 2) {
  const factor = 10 ** digits;
  return Math.round(n * factor) / factor;
}

function dayOfYear(date) {
  const start = new Date(date.getFullYear(), 0, 1);
  const diff = date - start;
  return Math.floor(diff / 86400000) + 1;
}

function circularDayDistance(a, b) {
  const diff = Math.abs(a - b);
  return Math.min(diff, 366 - diff);
}

function buildSeasonalPool(values, today, windowDays = 30) {
  const todayDoy = dayOfYear(today);

  return values
    .map((row) => {
      const date = parseDateOnly(row?.dateTime);
      const value = Number(row?.value);
      return { date, value };
    })
    .filter((row) => row.date && Number.isFinite(row.value))
    .filter((row) => circularDayDistance(dayOfYear(row.date), todayDoy) <= windowDays);
}

function parseDateOnly(s) {
  if (!s) return null;
  const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return null;
  return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
}

function percentile(arr, p) {
  if (!Array.isArray(arr) || arr.length === 0) return null;

  const sorted = [...arr].sort((a, b) => a - b);
  const idx = (sorted.length - 1) * p;
  const lower = Math.floor(idx);
  const upper = Math.ceil(idx);

  if (lower === upper) return sorted[lower];

  const weight = idx - lower;
  return sorted[lower] * (1 - weight) + sorted[upper] * weight;
}

function classifySeasonalFlow(currentFlow, p25, p75, p90) {
  if (!Number.isFinite(currentFlow) || !Number.isFinite(p25) || !Number.isFinite(p75) || !Number.isFinite(p90)) {
    return "Seasonal context unavailable";
  }

  if (currentFlow < p25) return "Below typical for season";
  if (currentFlow <= p75) return "Near typical for season";
  if (currentFlow <= p90) return "Above typical for season";
  return "High for season";
}

function buildNarrative(label) {
  switch (label) {
    case "Below typical for season":
      return "Current flow is below the typical range for this time of year based on the historical gauge record.";
    case "Near typical for season":
      return "Current flow is near the typical range for this time of year based on the historical gauge record.";
    case "Above typical for season":
      return "Current flow is above the typical range for this time of year based on the historical gauge record.";
    case "High for season":
      return "Current flow is high for this time of year based on the historical gauge record.";
    default:
      return "Seasonal flow context is not currently available.";
  }
}
