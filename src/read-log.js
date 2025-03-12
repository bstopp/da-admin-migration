import process from 'node:process';
import { readFile } from 'fs/promises';
import yargs from 'yargs';

const argv = yargs(process.argv.splice(2))
  .check((argv) => {
    const orgs = argv._;
    if (orgs.length !== 1) {
      throw new Error('An org must be specified')
    }
    return true;
  })
  .parse()

const org = argv._[0];

const results = await readFile(`migrate-${org}.results.json`, { encoding: 'utf8' }).then((data) => JSON.parse(data));
console.log('Successes:', results.success.length);
console.log('Failures:', results.failed.length);
