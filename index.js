import { PassThrough } from "node:stream";

import {
  CreateBucketCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { NodeHttpHandler } from "@aws-sdk/node-http-handler";
import pTimeout from "p-timeout";
import dotenv from "dotenv";

dotenv.config();

const BUCKET_NAME = "martin-slota-test";

class LoggingHttpHandler {
  constructor(options) {
    this.innerHandler = new NodeHttpHandler(options);
  }

  async handle(request, options) {
    const response = await this.innerHandler.handle(request, options);

    console.log("AWS SDK HTTP Response:", {
      statusCode: response.response.statusCode,
      headers: response.response.headers,
    });

    return response;
  }
}

const s3Client = new S3Client({
  endpoint: process.env.AWS_S3_ENDPOINT,
  requestHandler: new LoggingHttpHandler(),
});

s3Client.middlewareStack.add(
  (next, context) => async (args) => {
    console.log("AWS SDK context", context.clientName, context.commandName);
    console.log("AWS SDK request input", args.input);
    const result = await next(args);
    const { Body, ...rest } = result.output;
    console.log("AWS SDK request output:", { ...rest, Body: "REDACTED" });
    return result;
  },
  {
    name: "MyMiddleware",
    step: "build",
    override: true,
  }
);

async function ensureBucketExists() {
  try {
    await s3Client.send(new CreateBucketCommand({ Bucket: BUCKET_NAME }));
  } catch (e) {
    if (e.name !== "BucketAlreadyOwnedByYou") {
      throw e;
    }
  }
}

let sequenceNumber = 0;

async function uploadStream(body) {
  const key = `object_${sequenceNumber++}`;
  const upload = new Upload({
    client: s3Client,
    params: {
      Bucket: BUCKET_NAME,
      Key: key,
      Body: body,
    },
  });

  await upload.done();
  return key;
}

export async function uploadEmptyStream() {
  {
    const writer = new PassThrough();
    const uploadPromise = uploadStream(writer);

    // whoops, it turns out there is no data to write
    writer.end();

    await uploadPromise;
  }
}

async function putEmptyObject() {
  const key = `object_${sequenceNumber++}`;
  const putObject = new PutObjectCommand({
    Bucket: BUCKET_NAME,
    Key: key,
    Body: Buffer.from([]),
  });
  await s3Client.send(putObject);
}

await ensureBucketExists();

while (true) {
  // await pTimeout(uploadEmptyStream(), { milliseconds: 5_000 });
  await pTimeout(putEmptyObject(), { milliseconds: 5_000 });
}
