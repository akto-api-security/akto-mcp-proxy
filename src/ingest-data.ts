export interface IngestDataBatch {
  path: string;
  requestHeaders: string;
  responseHeaders: string;
  method: string;
  requestPayload: string;
  responsePayload: string;
  ip: string;
  destIp?: string;
  time: string;
  statusCode: string;
  type: string;
  status: string;
  akto_account_id: string;
  akto_vxlan_id: string;
  is_pending: string;
  source: string;
  direction?: string;
  process_id?: string;
  socket_id?: string;
  daemonset_id?: string;
  enabled_graph?: string;
  tag?: string;
}

export interface IngestDataRequest {
  batchData: IngestDataBatch[];
}

export interface IngestDataResult {
  success: boolean;
  message: string;
  processed: number;
}

export function ingestData(
  ingestDataRequest: IngestDataRequest,
  env: any,
  ctx: ExecutionContext
): IngestDataResult {
  try {
    if (!ingestDataRequest.batchData || ingestDataRequest.batchData.length === 0) {
      return {
        success: false,
        message: "batchData is required and must not be empty",
        processed: 0
      };
    }

    // Validate each batch item
    for (const batch of ingestDataRequest.batchData) {
      if (!batch.path || !batch.method || !batch.time) {
        return {
          success: false,
          message: "Missing required fields in batchData: path, method, and time are mandatory",
          processed: 0
        };
      }
    }

    ctx.waitUntil((async () => {
      try {
        await sendToQueue(ingestDataRequest.batchData, env);
      } catch (error) {
        console.error("Error processing traffic in background:", error);
      }
    })());

    return {
      success: true,
      message: "Traffic successfully queued for processing",
      processed: ingestDataRequest.batchData.length
    };
  } catch (error) {
    console.error("Error ingesting data:", error);
    return {
      success: false,
      message: `Error ingesting data: ${error instanceof Error ? error.message : 'Unknown error'}`,
      processed: 0
    };
  }
}

async function sendToQueue(batchData: IngestDataBatch[], env: any): Promise<void> {
  try {
    if (!batchData || batchData.length === 0) return;

    const messages = batchData.map((item: IngestDataBatch) => ({
      body: JSON.stringify(item),
    }));

    console.log(`Sending ${messages.length} message(s) to queue:`, JSON.stringify(messages, null, 2));
    await env.AKTO_TRAFFIC_QUEUE_MOTLEY_FOOL.send(messages);
    console.log(`Successfully sent ${messages.length} message(s) to queue`);
  } catch (err) {
    console.error("Failed to send to queue:", err);
  }
}
