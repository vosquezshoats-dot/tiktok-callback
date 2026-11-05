
const CALLBACK_URL =
  "https://tiktok-callback.vosquezshoats.workers.dev/api/tiktok/oauth/callback";

const TIKTOK_AUTH_URL =
  "https://business-api.tiktok.com/portal/auth";

const TIKTOK_TOKEN_URL =
  "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/";

const encoder = new TextEncoder();

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    }
  });
}

function hexToBytes(hex) {
  if (
    typeof hex !== "string" ||
    !/^(?:[0-9a-fA-F]{2})+$/.test(hex)
  ) {
    throw new Error("Invalid hexadecimal data");
  }

  const bytes = new Uint8Array(hex.length / 2);

  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(
      hex.slice(i * 2, i * 2 + 2),
      16
    );
  }

  return bytes;
}

function bytesToHex(bytes) {
  return Array.from(bytes)
    .map(byte => byte.toString(16).padStart(2, "0"))
    .join("");
}

async function sha256(value) {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    encoder.encode(value)
  );

  return bytesToHex(new Uint8Array(digest));
}

async function encryptToken(token, secret) {
  const keyBytes = hexToBytes(secret);

  const key = await crypto.subtle.importKey(
    "raw",
    keyBytes,
    { name: "AES-GCM" },
    false,
    ["encrypt"]
  );

  const nonce = crypto.getRandomValues(new Uint8Array(12));

  const encrypted = await crypto.subtle.encrypt(
    { name: "AES-GCM", iv: nonce },
    key,
    encoder.encode(token)
  );

  return {
    ciphertext: bytesToHex(new Uint8Array(encrypted)),
    nonce: bytesToHex(nonce)
  };
}
async function decryptToken(ciphertext, nonceHex, secret) {
  const keyBytes = hexToBytes(secret);

  const key = await crypto.subtle.importKey(
    "raw",
    keyBytes,
    { name: "AES-GCM" },
    false,
    ["decrypt"]
  );

  const decrypted = await crypto.subtle.decrypt(
    {
      name: "AES-GCM",
      iv: hexToBytes(nonceHex)
    },
    key,
    hexToBytes(ciphertext)
  );

  return new TextDecoder().decode(decrypted);
}
async function getAccessToken(env) {
  const credential = await env.DB.prepare(`
    SELECT encrypted_access_token, token_nonce
    FROM oauth_credentials
    WHERE app_id = ?
    ORDER BY id DESC
    LIMIT 1
  `).bind(env.TIKTOK_APP_ID).first();

  if (!credential) {
    throw new Error("No TikTok authorization record found.");
  }

  return await decryptToken(
    credential.encrypted_access_token,
    credential.token_nonce,
    env.TOKEN_ENCRYPTION_KEY
  );
}
async function startAuthorization(env) {
  const stateBytes = crypto.getRandomValues(
    new Uint8Array(32)
  );

  const state = bytesToHex(stateBytes);
  const stateHash = await sha256(state);

  const now = new Date();
  const expires = new Date(now.getTime() + 10 * 60 * 1000);

  await env.DB.prepare(`
    INSERT INTO oauth_sessions
    (state_hash, created_at, expires_at, used_at)
    VALUES (?, ?, ?, NULL)
  `).bind(
    stateHash,
    now.toISOString(),
    expires.toISOString()
  ).run();

  const authUrl = new URL(TIKTOK_AUTH_URL);

  authUrl.searchParams.set("app_id", env.TIKTOK_APP_ID);
  authUrl.searchParams.set("state", state);
  authUrl.searchParams.set("redirect_uri", CALLBACK_URL);

  return Response.redirect(authUrl.toString(), 302);
}

async function handleCallback(request, env) {
  const url = new URL(request.url);

  const authCode =
    url.searchParams.get("auth_code") ||
    url.searchParams.get("code");

  const state = url.searchParams.get("state");
  const error = url.searchParams.get("error");

  if (error) {
    return new Response(
      "TikTok authorization was not completed.",
      { status: 400 }
    );
  }

  if (!authCode || !state) {
    return new Response(
      "Missing authorization code or state.",
      { status: 400 }
    );
  }

  const stateHash = await sha256(state);

  const now = new Date().toISOString();

  const stateResult = await env.DB.prepare(`
    UPDATE oauth_sessions
    SET used_at = ?
    WHERE state_hash = ?
      AND used_at IS NULL
      AND expires_at > ?
  `).bind(
    now,
    stateHash,
    now
  ).run();

  if (!stateResult.meta || stateResult.meta.changes !== 1) {
    return new Response(
      "Invalid, expired, or already used authorization state.",
      { status: 403 }
    );
  }

  const tokenResponse = await fetch(TIKTOK_TOKEN_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      app_id: env.TIKTOK_APP_ID,
      secret: env.TIKTOK_APP_SECRET,
      auth_code: authCode
    })
  });

  const result = await tokenResponse.json();

  if (
    !tokenResponse.ok ||
    result.code !== 0 ||
    !result.data ||
    !result.data.access_token
  ) {
    console.error(
      "TikTok token exchange failed:",
      result.code || "Unknown error"
    );

    return new Response(
      "TikTok token exchange failed. Please start authorization again.",
      { status: 502 }
    );
  }

  const encrypted = await encryptToken(
    result.data.access_token,
    env.TOKEN_ENCRYPTION_KEY
  );

  const timestamp = new Date().toISOString();

  await env.DB.prepare(`
    INSERT INTO oauth_credentials (
      app_id,
      encrypted_access_token,
      token_nonce,
      encryption_version,
      created_at,
      updated_at
    )
    VALUES (?, ?, ?, 1, ?, ?)
  `).bind(
    env.TIKTOK_APP_ID,
    encrypted.ciphertext,
    encrypted.nonce,
    timestamp,
    timestamp
  ).run();

  return new Response(
    "TikTok authorization successful. Encrypted token saved.",
    {
      status: 200,
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "no-store"
      }
    }
  );
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response(
        "TikTok Marketing API Worker is running."
      );
    }

    if (url.pathname === "/api/tiktok/oauth/start") {
      if (!env.TIKTOK_APP_ID || !env.TIKTOK_APP_SECRET) {
        return new Response(
          "TikTok app configuration is incomplete.",
          { status: 500 }
        );
      }

      return startAuthorization(env);
    }

    if (url.pathname === "/api/tiktok/oauth/callback") {
      if (
        !env.TIKTOK_APP_ID ||
        !env.TIKTOK_APP_SECRET ||
        !env.TOKEN_ENCRYPTION_KEY ||
        !env.DB
      ) {
        return new Response(
          "Server configuration is incomplete.",
          { status: 500 }
        );
      }

      try {
        return await handleCallback(request, env);
      } catch (error) {
        console.error(
          "OAuth callback processing failed:",
          error.message
        );

        return new Response(
          "Internal server error. Please check Worker logs.",
          { status: 500 }
        );
      }
    }
    if (url.pathname === "/api/tiktok/test-token") {
      const providedKey = request.headers.get("X-Dashboard-Key");

      if (
        !env.DASHBOARD_API_KEY ||
        providedKey !== env.DASHBOARD_API_KEY
      ) {
        return new Response("Unauthorized", { status: 401 });
      }

      try {
        const token = await getAccessToken(env);

        return Response.json({
          success: true,
          token_decryption: "success",
          token_present: Boolean(token)
        });
      } catch (error) {
        console.error("Token test failed:", error.message);

        return Response.json({
          success: false,
          error: "Token decryption failed."
        }, { status: 500 });
      }
    }
        if (url.pathname === "/api/tiktok/business-centers") {
      const providedKey = request.headers.get("X-Dashboard-Key");

      if (
        !env.DASHBOARD_API_KEY ||
        providedKey !== env.DASHBOARD_API_KEY
      ) {
        return new Response("Unauthorized", { status: 401 });
      }

      try {
        const token = await getAccessToken(env);

        const apiResponse = await fetch(
          "https://business-api.tiktok.com/open_api/v1.3/bc/get/?page=1&page_size=10",
          {
            method: "GET",
            headers: {
              "Access-Token": token
            }
          }
        );

        const result = await apiResponse.json();

        return Response.json({
          success: apiResponse.ok && result.code === 0,
          http_status: apiResponse.status,
          tiktok_code: result.code ?? null,
          message: result.message ?? null,
          data: result.data ?? null
        });
      } catch (error) {
        console.error("Business Center query failed:", error.message);

        return Response.json({
          success: false,
          error: "Business Center query failed."
        }, { status: 500 });
      }
    }
      if (url.pathname === "/api/tiktok/sync-business-centers") {
  const providedKey = request.headers.get("X-Dashboard-Key");

  if (
    !env.DASHBOARD_API_KEY ||
    providedKey !== env.DASHBOARD_API_KEY
  ) {
    return new Response("Unauthorized", { status: 401 });
  }

  try {
    const token = await getAccessToken(env);

    const apiResponse = await fetch(
      "https://business-api.tiktok.com/open_api/v1.3/bc/get/?page=1&page_size=10",
      {
        method: "GET",
        headers: {
          "Access-Token": token
        }
      }
    );

    const result = await apiResponse.json();

    if (!apiResponse.ok || result.code !== 0) {
      return Response.json({
        success: false,
        error: "TikTok Business Center query failed.",
        tiktok_code: result.code ?? null,
        message: result.message ?? null
      }, { status: 502 });
    }

    const list = result.data?.list ?? [];
    let savedCount = 0;

    for (const item of list) {
      const bc = item.bc_info;

      if (!bc?.bc_id || !bc?.name) {
        continue;
      }

      await env.DB.prepare(`
        INSERT INTO business_centers (
          business_center_id,
          name,
          updated_at
        )
        VALUES (?, ?, ?)
        ON CONFLICT(business_center_id)
        DO UPDATE SET
          name = excluded.name,
          updated_at = excluded.updated_at
      `).bind(
        bc.bc_id,
        bc.name,
        new Date().toISOString()
      ).run();

      savedCount++;
    }

    return Response.json({
      success: true,
      fetched: list.length,
      saved: savedCount
    });
  } catch (error) {
    console.error("Business Center sync failed:", error.message);

    return Response.json({
      success: false,
      error: "Business Center sync failed."
    }, { status: 500 });
  }
} 
      if (url.pathname === "/api/tiktok/daily-spend") {
      const providedKey = request.headers.get("X-Dashboard-Key");

      if (
        !env.DASHBOARD_API_KEY ||
        providedKey !== env.DASHBOARD_API_KEY
      ) {
        return new Response("Unauthorized", { status: 401 });
      }

      const startDate = url.searchParams.get("start_date");
      const endDate = url.searchParams.get("end_date");
      const businessCenterId = url.searchParams.get("bc_id");

      if (
        !startDate ||
        !endDate ||
        !/^\d{4}-\d{2}-\d{2}$/.test(startDate) ||
        !/^\d{4}-\d{2}-\d{2}$/.test(endDate) ||
        startDate > endDate
      ) {
        return Response.json({
          success: false,
          error: "Please provide valid start_date and end_date."
        }, { status: 400 });
      }

      try {
        let query = `
          SELECT
            business_center_id,
            advertiser_id,
            report_date,
            currency,
            spend
          FROM daily_spend
          WHERE report_date >= ?
            AND report_date <= ?
        `;

        const params = [startDate, endDate];

        if (businessCenterId) {
          query += " AND business_center_id = ?";
          params.push(businessCenterId);
        }

        query += `
          ORDER BY
            report_date,
            business_center_id,
            advertiser_id,
            currency
        `;

        const result = await env.DB.prepare(query)
          .bind(...params)
          .all();

        return Response.json({
          success: true,
          count: result.results.length,
          data: result.results
        });
      } catch (error) {
        console.error("Daily spend query failed:", error.message);

        return Response.json({
          success: false,
          error: "Daily spend query failed."
        }, { status: 500 });
      }
    }
  
if (url.pathname === "/api/tiktok/sync-business-center-accounts") {
  const providedKey = request.headers.get("X-Dashboard-Key");

  if (
    !env.DASHBOARD_API_KEY ||
    providedKey !== env.DASHBOARD_API_KEY
  ) {
    return new Response("Unauthorized", { status: 401 });
  }

  const businessCenterId = url.searchParams.get("bc_id");
  const page = Number(url.searchParams.get("page") || "1");

  if (!businessCenterId || !Number.isInteger(page) || page < 1) {
    return Response.json({
      success: false,
      error: "Please provide a valid bc_id and page."
    }, { status: 400 });
  }

  try {
    const token = await getAccessToken(env);

    // 1. 获取当前 BC 的一页广告账户
    const assetUrl = new URL(
  "https://business-api.tiktok.com/open_api/v1.3/bc/asset/get/"
);

    assetUrl.searchParams.set("bc_id", businessCenterId);
    assetUrl.searchParams.set("asset_type", "ADVERTISER");
    assetUrl.searchParams.set("page", String(page));
    assetUrl.searchParams.set("page_size", "50");

    const assetResponse = await fetch(assetUrl.toString(), {
      method: "GET",
      headers: {
        "Access-Token": token
      }
    });

    const assetResult = await assetResponse.json();

    if (!assetResponse.ok || assetResult.code !== 0) {
      return Response.json({
        success: false,
        step: "asset_query",
        message: assetResult.message ?? null,
        tiktok_code: assetResult.code ?? null
      }, { status: 502 });
    }

    const assets = assetResult.data?.list ?? [];
    const pageInfo = assetResult.data?.page_info ?? {};
    const advertiserIds = assets
      .map(item => item.asset_id)
      .filter(Boolean);

    if (advertiserIds.length === 0) {
      return Response.json({
        success: true,
        bc_id: businessCenterId,
        page,
        total_pages: pageInfo.total_page ?? 0,
        fetched: 0,
        saved: 0
      });
    }

    // 2. 每 20 个账户查询一次详细信息
    const advertiserInfoMap = new Map();

    for (let i = 0; i < advertiserIds.length; i += 20) {
      const batchIds = advertiserIds.slice(i, i + 20);

     const infoUrl = new URL(
  "https://business-api.tiktok.com/open_api/v1.3/advertiser/info/"
);
      infoUrl.searchParams.set(
        "advertiser_ids",
        JSON.stringify(batchIds)
      );

      const infoResponse = await fetch(infoUrl.toString(), {
        method: "GET",
        headers: {
          "Access-Token": token
        }
      });

      const infoResult = await infoResponse.json();

      if (!infoResponse.ok || infoResult.code !== 0) {
        return Response.json({
          success: false,
          step: "advertiser_info",
          message: infoResult.message ?? null,
          tiktok_code: infoResult.code ?? null
        }, { status: 502 });
      }

      for (const advertiser of infoResult.data?.list ?? []) {
        advertiserInfoMap.set(
          String(advertiser.advertiser_id),
          advertiser
        );
      }
    }

    // 3. 保存账户到 D1
    const statements = [];
    const missingDetails = [];

    for (const asset of assets) {
      const advertiserId = String(asset.asset_id);
      const info = advertiserInfoMap.get(advertiserId);

      if (!info?.currency) {
        missingDetails.push(advertiserId);
        continue;
      }

      const advertiserName = info.name || asset.asset_name;

      if (!advertiserName) {
        missingDetails.push(advertiserId);
        continue;
      }

      statements.push(
        env.DB.prepare(`
          INSERT INTO advertiser_accounts (
            business_center_id,
            advertiser_id,
            advertiser_name,
            currency,
            updated_at
          )
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(business_center_id, advertiser_id)
          DO UPDATE SET
            advertiser_name = excluded.advertiser_name,
            currency = excluded.currency,
            updated_at = excluded.updated_at
        `).bind(
          businessCenterId,
          advertiserId,
          advertiserName,
          info.currency,
          new Date().toISOString()
        )
      );
    }

    if (statements.length > 0) {
      await env.DB.batch(statements);
    }

    return Response.json({
      success: true,
      bc_id: businessCenterId,
      page,
      total_pages: pageInfo.total_page ?? null,
      total_accounts: pageInfo.total_number ?? null,
      fetched: assets.length,
      saved: statements.length,
      missing_details: missingDetails
    });

  } catch (error) {
    console.error(
      "Business Center account sync failed:",
      error.message
    );

    return Response.json({
      success: false,
      error: "Business Center account sync failed."
    }, { status: 500 });
  }
}
 
if (url.pathname === "/api/tiktok/business-center-accounts") {
  const providedKey = request.headers.get("X-Dashboard-Key");

  if (
    !env.DASHBOARD_API_KEY ||
    providedKey !== env.DASHBOARD_API_KEY
  ) {
    return new Response("Unauthorized", { status: 401 });
  }

  const businessCenterId = url.searchParams.get("bc_id");

  if (!businessCenterId) {
    return Response.json({
      success: false,
      error: "Please provide bc_id."
    }, { status: 400 });
  }

  try {
    const token = await getAccessToken(env);

    const apiUrl = new URL(
      "https://business-api.tiktok.com/open_api/v1.3/bc/asset/get/"
    );
    apiUrl.searchParams.set("bc_id", businessCenterId);
    apiUrl.searchParams.set("asset_type", "ADVERTISER");

    const page = url.searchParams.get("page") || "1";
    apiUrl.searchParams.set("page", page);

    apiUrl.searchParams.set("page_size", "50");

    const apiResponse = await fetch(apiUrl.toString(), {
      method: "GET",
      headers: {
        "Access-Token": token
      }
    });

    const result = await apiResponse.json();

    return Response.json({
      success: apiResponse.ok && result.code === 0,
      http_status: apiResponse.status,
      tiktok_code: result.code ?? null,
      message: result.message ?? null,
      data: result.data ?? null
    });
  } catch (error) {
    console.error("Advertiser account query failed:", error.message);

    return Response.json({
      success: false,
      error: "Advertiser account query failed."
    }, { status: 500 });
  }
}

if (url.pathname === "/api/tiktok/advertiser-info") {
    const advertiserId = url.searchParams.get("advertiser_id");
  const advertiserIdsParam = url.searchParams.get("advertiser_ids");

  const advertiserIds = advertiserIdsParam
    ? advertiserIdsParam.split(",").map(id => id.trim()).filter(Boolean)
    : advertiserId
      ? [advertiserId]
      : [];

  if (advertiserIds.length === 0) {
    return Response.json({
      success: false,
      error: "Please provide advertiser_id or advertiser_ids."
    }, { status: 400 });
  }

  if (advertiserIds.length > 20) {
    return Response.json({
      success: false,
      error: "A maximum of 20 advertiser IDs is allowed per request."
    }, { status: 400 });
  }

  try {
    const token = await getAccessToken(env);

    const apiUrl = new URL(
      "https://business-api.tiktok.com/open_api/v1.3/advertiser/info/"
    );

  apiUrl.searchParams.set(
  "advertiser_ids",
  JSON.stringify(advertiserIds)
 );

    const apiResponse = await fetch(apiUrl.toString(), {
      method: "GET",
      headers: {
        "Access-Token": token
      }
    });

    const result = await apiResponse.json();

    return Response.json({
      success: apiResponse.ok && result.code === 0,
      http_status: apiResponse.status,
      tiktok_code: result.code ?? null,
      message: result.message ?? null,
      data: result.data ?? null
    });
  } catch (error) {
    console.error("Advertiser info query failed:", error.message);

    return Response.json({
      success: false,
      error: "Advertiser info query failed."
    }, { status: 500 });
  }
}

    return new Response("Not Found", { status: 404 });
  }
};
