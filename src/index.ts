import { Hono } from "hono";
import { ingestDataDeprecated, IngestDataRequestDeprecated, IIngestDataRequestDeprecated } from "./ingest-data-deprecated";
import { ingestData, IngestDataRequest } from "./ingest-data";

export interface Env {
  // Queue binding
  AKTO_TRAFFIC_QUEUE?: Queue;
}

const app = new Hono<{ Bindings: Env }>();

// Health check endpoint
app.get("/health", (c) => {
  return c.text("OK");
});

// New traffic ingestion endpoint (v2)
app.post("/api/ingestData", async (c) => {
  try {
    const requestBody = await c.req.json<IngestDataRequest>();

    // Validate request body
    if (!requestBody.batchData || !Array.isArray(requestBody.batchData)) {
      return c.json(
        { error: "batchData is required and must be an array" },
        400
      );
    }

    if (requestBody.batchData.length === 0) {
      return c.json(
        { error: "batchData must contain at least one item" },
        400
      );
    }

    const result = ingestData(requestBody, c.env, c.executionCtx);

    if (!result.success) {
      return c.json({ error: result.message }, 400);
    }

    return c.json({
      message: result.message,
      processed: result.processed,
    });
  } catch (error) {
    console.error("Error processing ingestData request:", error);

    if (error instanceof SyntaxError) {
      return c.json({ error: "Invalid JSON in request body" }, 400);
    }

    return c.json({ error: "Internal server error while processing request" }, 500);
  }
});

// Traffic ingestion endpoint (deprecated - use /api/ingestData instead)
app.post("/ingest-data", async (c) => {
  try {
    const requestBody = await c.req.json<IIngestDataRequestDeprecated>();

    // Validate required fields
    if (!requestBody.host || !requestBody.url || !requestBody.method) {
      return c.json(
        { error: "Missing required fields: host, url, and method are mandatory" },
        400
      );
    }

    if (!requestBody.requestHeaders || !requestBody.responseHeaders) {
      return c.json(
        { error: "Missing required fields: requestHeaders and responseHeaders are mandatory" },
        400
      );
    }

    if (typeof requestBody.responseStatus !== "number") {
      return c.json({ error: "responseStatus must be a number" }, 400);
    }

    // Ensure host is in requestHeaders
    if (!requestBody.requestHeaders.host) {
      requestBody.requestHeaders.host = requestBody.host;
    }

    const ingestDataRequest = new IngestDataRequestDeprecated(
      requestBody.host,
      requestBody.url,
      requestBody.method,
      requestBody.requestHeaders,
      requestBody.requestBody || "",
      requestBody.responseHeaders,
      requestBody.responseStatus,
      requestBody.responseStatusText || "",
      requestBody.responseBody || "",
      requestBody.time,
      requestBody.tag
    );

    const result = ingestDataDeprecated(ingestDataRequest, c.env, c.executionCtx);

    if (!result.success) {
      return c.json({ error: result.message }, 500);
    }

    return c.json({
      message: result.message,
      captured: result.captured,
    });
  } catch (error) {
    console.error("Error processing ingest-data request:", error);

    if (error instanceof SyntaxError) {
      return c.json({ error: "Invalid JSON in request body" }, 400);
    }

    return c.json({ error: "Internal server error while processing request" }, 500);
  }
});

// 404 handler
app.notFound((c) => {
  return c.json(
    {
      error: "Not Found",
      message: "This worker handles /health, /api/ingestData, and /ingest-data (deprecated) endpoints",
    },
    404
  );
});

export default app;
