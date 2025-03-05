# DA Admin Migration Tool

This is intended to migrate an org from one DA Admin to another.

## Usage

1. Create file `.dev.vars` in the root of the project with the following content. Fill in the structure with the appropriate values (should be self-explanatory).

```json
{
  "bearer": "",
  "source": {
    "daAdminUrl": "",
    "region": "auto",
    "endpoint": "",
    "credentials": {
      "accessKeyId": "",
      "secretAccessKey": ""
    }
  },
  "dest": {
    "daAdminUrl": "",
    "region": "auto",
    "endpoint": "",
    "credentials": {
      "accessKeyId": "",
      "secretAccessKey": ""
    }
  }
}
```

2. Run `npm install`


3. Run `npm run migrate <org>`
