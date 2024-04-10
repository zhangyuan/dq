# dq

A data quality utility.

> This project is a personal project that is updated intermittently.

## Installation

Download the binary from [Releases](https://github.com/zhangyuan/dq/releases) and rename it to `dq`. Or build the binary with Golang and `make`:

```
make build
```

## Usage (Example)

### Launch the postgres database

```sh
docker-compose up
```

### Prepare the database and table

```sql
CREATE TABLE posts (
  title VARCHAR(64),
  content TEXT,
  author VARCHAR(64),
  status VARCHAR(64),
  _deleted BOOL
);

INSERT INTO posts(title, content, author, status, _deleted)
VALUES
    ('title 1', 'What a great day', 'Jack', 'Private', false),
    ('title 2', 'What a great day', 'Jack', 'Draft', false),
    ('title 3', 'What a great day', 'Jack', 'Published', false),
    ('title 2', 'What a great day', NULL, 'Published', false);
```

### Prepare the env file

```.env
DSN="postgres://dq:dq@localhost:5432/postgres?sslmode=disable"
DEBUG=false
```

### Prepare the spec configuration

Use [examples/dq.yaml](examples/dq.yaml) as below:

```yaml
models:
  - table: posts
    filter: _deleted = false
    columns:
      - name: title
        tests:
          - unique
      - name: author
        tests:
          - not_null
      - name: status
        tests:
          - name: status must be in accepted values
            sql: |
              SELECT * FROM posts WHERE status NOT IN ('Draft', 'Private', 'Published')
```

### Run dq to check the data quality

#### With the default output format plaintext

```bash
./dq check -s examples/dq.yaml
```

and the output as below:

```
posts
- title should be unique [OK]
- author should not be null [OK]
- status must be in accepted values  [OK]
```

#### With the output format json

```bash
./dq check -s examples/dq.yaml -f json | jq
```

and the output as below:

```json
{
  "models": [
    {
      "model": "posts",
      "columns": [
        {
          "column": "title",
          "tests": [
            {
              "spec": "unique",
              "title": "title should be unique",
              "sql": "SELECT COUNT(*) rows_count, COUNT(DISTINCT title) distinct_rows_count FROM posts WHERE _deleted = false",
              "is_ok": true,
              "info": {
                "distinct_rows_count": 3,
                "rows_count": 3
              }
            }
          ]
        },
        {
          "column": "author",
          "tests": [
            {
              "spec": "not_null",
              "title": "author should not be null",
              "sql": "SELECT COUNT(*) rows_count FROM posts WHERE author IS NULL AND _deleted = false",
              "is_ok": true,
              "info": {
                "rows_count": 0
              }
            }
          ]
        },
        {
          "column": "status",
          "tests": [
            {
              "spec": {
                "name": "status must be in accepted values",
                "sql": "SELECT * FROM posts WHERE status NOT IN ('Draft', 'Private', 'Published')\n"
              },
              "title": "status must be in accepted values ",
              "sql": "SELECT COUNT(*) FROM (SELECT * FROM posts WHERE status NOT IN ('Draft', 'Private', 'Published')\n) a",
              "is_ok": true,
              "info": {
                "rows_count": 0
              }
            }
          ]
        }
      ]
    }
  ]
}
```
