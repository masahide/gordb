gordb
=====

[![Circle CI](https://circleci.com/gh/masahide/gordb.svg?style=svg)](https://circleci.com/gh/masahide/gordb) [![Build Status](https://drone.io/github.com/masahide/gordb/status.png)](https://drone.io/github.com/masahide/gordb/latest) [![Coverage Status](https://coveralls.io/repos/masahide/gordb/badge.svg?branch=master&service=github)](https://coveralls.io/github/masahide/gordb?branch=master)


### Make

```
go build -ldflags "-X main.version $(git describe)"
```

### Usage

```
$ curl -X POST localhost:3050/query/dir2 -d '
[
  {
    "union": {
      "input1": {"selection": {
        "input": { "relation": { "name": "staff" } },
        "attr": "age",  "selector": ">=", "arg": 31
      }},
      "input2": {"selection": {
        "input": { "relation": { "name": "staff" } },
        "attr": "name", "selector": "==", "arg": "山田"
      }}
    }
  },
  {
    "selection": {
      "input": { "relation": { "name": "rank" } },
      "attr": "rank",  "selector": ">=", "arg": 1
    }
  }
]
'
```

result:
```
[{"attrs":["name","age","job"],"data":[["田中",34,"デザイナー"]]},{"attrs":["name","rank"],"data":[["清水",78],["田中",46],["佐藤",33]]}]
```

### Relational database term

https://en.wikipedia.org/wiki/Relational_database#Terminology


