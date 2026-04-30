import { put } from '@vercel/blob';

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader(
    'Cache-Control',
    'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0, s-maxage=0'
  );
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');

  try {
    const payload = {
      site: 'USGS-01618100',
      generatedAt: new Date().toISOString(),
      sourceUrl: 'manual-seed',
      latestDate: '2026-02-20T00:00:00.000Z',
      parameterCount: 7,
      parameters: [
        {
          characteristic: 'Temperature, water',
          group: 'physical',
          latest: {
            characteristic: 'Temperature, water',
            group: 'physical',
            value: 8.6,
            unit: 'deg C',
            date: '2026-02-20T00:00:00.000Z'
          },
          count: 1,
          series: [
            {
              characteristic: 'Temperature, water',
              group: 'physical',
              value: 8.6,
              unit: 'deg C',
              date: '2026-02-20T00:00:00.000Z'
            }
          ]
        },
        {
          characteristic: 'Specific conductance',
          group: 'physical',
          latest: {
            characteristic: 'Specific conductance',
            group: 'physical',
            value: 567,
            unit: 'uS/cm',
            date: '2026-02-20T00:00:00.000Z'
          },
          count: 1,
          series: [
            {
              characteristic: 'Specific conductance',
              group: 'physical',
              value: 567,
              unit: 'uS/cm',
              date: '2026-02-20T00:00:00.000Z'
            }
          ]
        },
        {
          characteristic: 'Phosphorus',
          group: 'nutrient',
          latest: {
            characteristic: 'Phosphorus',
            group: 'nutrient',
            value: 0.04,
            unit: 'mg/L',
            date: '2026-02-20T00:00:00.000Z'
          },
          count: 1,
          series: [
            {
              characteristic: 'Phosphorus',
              group: 'nutrient',
              value: 0.04,
              unit: 'mg/L',
              date: '2026-02-20T00:00:00.000Z'
            }
          ]
        },
        {
          characteristic: 'Nitrate',
          group: 'nutrient',
          latest: {
            characteristic: 'Nitrate',
            group: 'nutrient',
            value: 2.31,
            unit: 'mg/L',
            date: '2026-02-20T00:00:00.000Z'
          },
          count: 1,
          series: [
            {
              characteristic: 'Nitrate',
              group: 'nutrient',
              value: 2.31,
              unit: 'mg/L',
              date: '2026-02-20T00:00:00.000Z'
            }
          ]
        },
        {
          characteristic: 'Nitrite',
          group: 'nutrient',
          latest: {
            characteristic: 'Nitrite',
            group: 'nutrient',
            value: 0.023,
            unit: 'mg/L',
            date: '2026-02-20T00:00:00.000Z'
          },
          count: 1,
          series: [
            {
              characteristic: 'Nitrite',
              group: 'nutrient',
              value: 0.023,
              unit: 'mg/L',
              date: '2026-02-20T00:00:00.000Z'
            }
          ]
        },
        {
          characteristic: 'Inorganic nitrogen (nitrate and nitrite)',
          group: 'nutrient',
          latest: {
            characteristic: 'Inorganic nitrogen (nitrate and nitrite)',
            group: 'nutrient',
            value: 2.33,
            unit: 'mg/L',
            date: '2026-02-20T00:00:00.000Z'
          },
          count: 1,
          series: [
            {
              characteristic: 'Inorganic nitrogen (nitrate and nitrite)',
              group: 'nutrient',
              value: 2.33,
              unit: 'mg/L',
              date: '2026-02-20T00:00:00.000Z'
            }
          ]
        },
        {
          characteristic: 'Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)',
          group: 'nutrient',
          latest: {
            characteristic: 'Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)',
            group: 'nutrient',
            value: 2.69,
            unit: 'mg/L',
            date: '2026-02-20T00:00:00.000Z'
          },
          count: 1,
          series: [
            {
              characteristic: 'Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)',
              group: 'nutrient',
              value: 2.69,
              unit: 'mg/L',
              date: '2026-02-20T00:00:00.000Z'
            }
          ]
        }
      ]
    };

    await put(
      'snapshots/rocky-marsh-wq.json',
      JSON.stringify(payload, null, 2),
      {
        access: 'public',
        addRandomSuffix: false,
        allowOverwrite: true,
        contentType: 'application/json'
      }
    );

    return res.status(200).json({
      ok: true,
      message: 'Fallback snapshot seeded',
      latestDate: payload.latestDate,
      parameterCount: payload.parameterCount
    });
  } catch (err) {
    return res.status(500).json({
      error: err.message || 'Unknown error'
    });
  }
}
