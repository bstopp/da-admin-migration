import process, { stdout } from 'node:process';
import { readFile, writeFile } from 'fs/promises';
import yargs from 'yargs';
import { GetObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';

const DEST_BUCKET = 'da-content';
const MaxKeys = 100;

async function createOrg(config, org) {
  let resp = await fetch(`${config.source.daAdminUrl}/list`);
  if (!resp.ok) throw new Error('Could not fetch org list.');

  let orgs = await resp.json();
  const created = orgs.find((o) => o.name === org)?.created;
  if (!created) throw new Error('Could not find org in source list.');
  const body = JSON.stringify({
    total: 1,
    limit: 1,
    offset: 0,
    data: [{
      created,
    }],
  });

  console.log(`Creating temporary ${org} file, to update DA_AUTH.`);
  // Have to use the Admin API.
  resp = await fetch(`${config.dest.daAdminUrl}/source/${org}/migration`, { method: 'POST', body });
  if (!resp.ok) throw new Error('Could not create migration props file.');
  resp = await fetch(`${config.dest.daAdminUrl}/list`);
  if (!resp.ok) throw new Error('Could not fetch org list.');
  orgs = await resp.json();
  if (!orgs.some((o) => o.name === org)) throw new Error('Could not find new org in list.');
  resp = await fetch(`${config.dest.daAdminUrl}/source/${org}/migration`, { method: 'DELETE' });
  if (!resp.ok) throw new Error('Could not delete org migration props file.');
}

async function migrateOrgConfig(config, org) {
  const getOpts = {};
  if (config.bearer) getOpts.headers = { Authorization: `Bearer ${config.bearer}` };

  const getResp = await fetch(`${config.source.daAdminUrl}/config/${org}`, getOpts);
  if (getResp.status === 404) {
    console.log('No config to migrate.');
    return;
  }
  if (!getResp.ok) {
    if (getResp.status === 403) {
      console.log('Skipping config as not authorized.');
      return;
    }
    throw new Error('Could not fetch source config.', getResp.status);
  }

  const data = await getResp.text();
  const body = new FormData();
  body.append('config', data);

  const postOpts = { method: 'POST', body };
  if (config.bearer) postOpts.headers = { Authorization: `Bearer ${config.bearer}` };

  const postResp = await fetch(`${config.dest.daAdminUrl}/config/${org}`, postOpts);
  if (!postResp.ok) throw new Error('Could not create org config.');
}

async function migrateSiteConfig(config, org) {
  const getOpts = {};
  if (config.bearer) getOpts.headers = { Authorization: `Bearer ${config.bearer}` };
  let getResp = await fetch(`${config.source.daAdminUrl}/list/${org}`, getOpts);
  if (!getResp.ok) {
    if (getResp.status === 401 || getResp.status === 403) {
      console.log(`Skipping site configs for ${org} as not authorized.`);
      return;
    }
    throw new Error('Could not list Org sites.', getResp.status);
  }

  const data = await getResp.json();
  const sites = data.filter((entry) => !entry.ext);

  for (const { name: site } of sites) {
    console.log('Migrating site config', site);
    getResp = await fetch(`${config.source.daAdminUrl}/config/${org}/${site}`, getOpts);
    if (!getResp.ok) {
      if (getResp.status === 404) {
        console.log(`No config for ${site} to migrate.`);
        return;
      }
      if (getResp.status === 401 || getResp.status === 403) {
        console.log(`Skipping config for ${site} as not authorized.`);
        return;
      }
      throw new Error('Could not fetch config for site', site, getResp.status);
    }

    const data = await resp.text();
    const body = new FormData();
    body.append('config', data);

    const postOpts = { method: 'POST', body };
    if (config.bearer) postOpts.headers = { Authorization: `Bearer ${config.bearer}` };

    const postResp = await fetch(`${config.dest.daAdminUrl}/config/${org}/${site}`, postOpts);
    if (!postResp.ok) throw new Error('Could not create config for site', site, postResp.status);
  }
}

async function listSourceContent(client, org, ContinuationToken) {
  const cmd = new ListObjectsV2Command({
    Bucket: `${org}-content`,
    MaxKeys,
    ContinuationToken,
  });

  const resp = await client.send(cmd);
  if (resp.$metadata.httpStatusCode !== 200) throw new Error('Unable to list source content.', resp.$metadata.httpStatusCode);
  const { Contents = [], NextContinuationToken } = resp;
  const files = Contents.map((c) => c.Key);
  return { files, continuation: NextContinuationToken };
}

async function copyFile(srcClient, destClient, org, Key) {

  return new Promise(async (resolve, reject) => {
    try {
      const cmd = new GetObjectCommand({
        Bucket: `${org}-content`,
        Key,
      });

      const getResp = await srcClient.send(cmd);
      if (getResp.$metadata.httpStatusCode !== 200) {
        reject(Key);
      }
      const { Body, ContentType, ContentLength, Metadata } = getResp;
      const input = {
        Bucket: DEST_BUCKET,
        Key: `${org}/${Key}`,
        Body,
        ContentType,
        ContentLength,
        Metadata,
      };

      const putCmd = new PutObjectCommand(input);
      const putResp = await destClient.send(putCmd);
      if (putResp.$metadata.httpStatusCode !== 200) {
        reject(Key);
      }

      resolve(Key);
    } catch (e) {
      reject(Key);
    }
  });
}

async function migrateContent(config, org) {
  const status = {
    org,
    success: [],
    failed: [],
  }
  const srcClient = new S3Client(config.source);
  const destClient = new S3Client(config.dest);
  let token = undefined;
  let i = 0;
  do {
    let { files, continuation } = await listSourceContent(srcClient, org, token);
    token = continuation;

    const promises = [];
    for (const file of files) {
      promises.push(copyFile(srcClient, destClient, org, file));
    }
    await Promise.allSettled(promises).then((results) => {
      results.forEach((result) => result.value ? status.success.push(result.value) : status.failed.push(result.reason));
    });

    stdout.clearLine(0);
    stdout.cursorTo(0);
    stdout.write(`Copied ${i * MaxKeys + files.length} files.`);
    i++;
  } while (token);
  stdout.write('\n');
  return status;
}

async function retryMigration(config, org) {
  const status = {
    org,
    success: [],
    failed: [],
  }
  const srcClient = new S3Client(config.source);
  const destClient = new S3Client(config.dest);
  const files = await readFile(`migrate-${org}.results.json`, { encoding: 'utf8' }).then((data) => JSON.parse(data).failed);
  let i = 0;

  do {
    const promises = [];
    while (i < MaxKeys && i < files.length) {
      promises.push(copyFile(srcClient, destClient, org, files[i]));
      i++;
    }
    await Promise.allSettled(promises).then((results) => {
      results.forEach((result) => result.value ? status.success.push(result.value) : status.failed.push(result.reason));
    });

    stdout.clearLine(0);
    stdout.cursorTo(0);
    stdout.write(`Copied ${i} files.`);
  } while (i < files.length);
  stdout.write('\n');
  return status;
}

const config = await readFile('.dev.vars', { encoding: 'utf8' }).then((data) => JSON.parse(data));

const argv = yargs(process.argv.splice(2))
  .check((argv) => {
    const args = argv._;
    if (args.length <= 1) {
      throw new Error('An org must be specified')
    }
    return true;
  })
  .parse()

const org = argv._[0];
const retry = argv._[1] === 'retry';

let results;
if (!retry) {
  console.log('Migrating', org)
  await createOrg(config, org);
  await migrateOrgConfig(config, org);
  await migrateSiteConfig(config, org);
  // results = await migrateContent(config, org);
  await writeFile(`migrate-${org}.results.json`, JSON.stringify(results, null, 2));
  console.log('Successes:', results.success.length);
  console.log('Failures:', results.failed.length);
  console.log('Migration complete.');
} else {
  console.log('Retrying failures in migration of', org);
  results = await retryMigration(config, org);
  await writeFile(`retry-${org}.results.json`, JSON.stringify(results, null, 2));
  console.log('Successes:', results.success.length);
  console.log('Failures:', results.failed.length);
  console.log('Retry complete.');

}

