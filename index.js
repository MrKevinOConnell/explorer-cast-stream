import express from 'express';
import { HubEventType, NobleEd25519Signer, bytesToHexString, getSSLHubRpcClient } from '@farcaster/hub-nodejs';
import dotenv from 'dotenv';
import cors from 'cors';

dotenv.config();
const app = express();
const port = process.env.PORT || 3000;

app.use(cors());

const onMessage = (message, subscribers) => {
  subscribers.forEach(subscriber => {
    subscriber.write(`data: ${JSON.stringify(message)}\n\n`);
  });
};

app.get('/stream', async (req, res) => {
  const hubRpcEndpoint = process.env.NEYNAR_GPRC_ENDPOINT;
  const client = getSSLHubRpcClient(hubRpcEndpoint);

  let subscribers = [];

  client.$.waitForReady(Date.now() + 5000, async (e) => {
    if (e) {
      console.error(`Failed to connect to ${hubRpcEndpoint}:`, e);
      res.status(500).send('Failed to connect');
      return;
    }

    try {
      const subscribeResult = await client.subscribe({
        eventTypes: [HubEventType.MERGE_MESSAGE],
      });

      if (subscribeResult.isOk()) {
        const stream = subscribeResult.value;

        for await (const event of stream) {
          if (event.mergeMessageBody.message.data.type === 1) {
            let hash = bytesToHexString(event.mergeMessageBody.message.hash);
            if (hash && hash.value) {
              hash = hash.value;
            }
            const fid = event.mergeMessageBody.message.data.fid;
            if (fid > 200000) {
              continue;
            }

            // Fetch author data
            const authorData = await client.getUserDataByFid({ fid });
            let author = {};
            if (authorData.isOk()) {
              authorData.value.messages.forEach((message) => {
                if (!message?.data?.userDataBody) return;
                const { type, value } = message.data.userDataBody;
                switch (type) {
                  case 1:
                    author.pfp_url = value;
                    break;
                  case 2:
                    author.display_name = value;
                    break;
                  case 3:
                    author.bio = value;
                    break;
                  case 6:
                    author.username = value;
                    break;
                  default:
                    break;
                }
              });
            }

            onMessage({ ...event.mergeMessageBody.message.data.castAddBody, fid, hash, author }, subscribers);
          }
        }
      }
    } catch (error) {
      console.error('Subscription error:', error);
      res.status(500).send('Subscription error');
      return;
    }
  });

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders(); // flush the headers to establish SSE with the client

  subscribers.push(res);

  res.write(': ping\n\n'); // Send initial ping to open the stream

  req.on('close', () => {
    subscribers = subscribers.filter(subscriber => subscriber !== res);
    console.log('Client disconnected');
    if (subscribers.length === 0) {
      client.close();
    }
  });
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
