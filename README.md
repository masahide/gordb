gordb
=====

[![Circle CI](https://circleci.com/gh/masahide/gordb.svg?style=svg)](https://circleci.com/gh/masahide/gordb) [![Build Status](https://drone.io/github.com/masahide/gordb/status.png)](https://drone.io/github.com/masahide/gordb/latest) [![Coverage Status](https://coveralls.io/repos/masahide/gordb/badge.svg?branch=master&service=github)](https://coveralls.io/github/masahide/gordb?branch=master)


### Make

```
go build -ldflags "-X main.version=$(git describe)"
```

### Usage

#### json

```
$ curl -X POST localhost:3050/json/dir2 -d '
[
   {
    "stream": {
      "union": {
        "input1": {"iselection": {
          "input": { "name": "staff" },
          "attr": "age",  "selector": ">=", "arg": 31
        }},
        "input2": {"iselection": {
          "input": { "name": "staff" },
          "attr": "name", "selector": "==", "arg": "山田"
        }}
      }
    }
  },
  {
    "stream": {
      "iselection": {
        "input": { "name": "rank" },
        "attr": "rank",  "selector": ">=", "arg": 1
      }
    }
  }
]
'
```

result:
```
[{"attrs":["name","age","job"],"data":[["田中",34,"デザイナー"]]},{"attrs":["name","rank"],"data":[["清水",78],["田中",46],["佐藤",33]]}]
```

#### php

```
$ curl -X POST localhost:3050/php/dir2 -d '
[
   {
    "options": { "kv": true, "map_key": "name" },
    "stream": {
      "union": {
        "input1": {"iselection": {
          "input": { "name": "staff" },
          "attr": "age",  "selector": ">=", "arg": 31
        }},
        "input2": {"iselection": {
          "input": { "name": "staff" },
          "attr": "name", "selector": "==", "arg": "山田"
        }}
      }
    }
  },
  {
    "options": { "kv": true, "map_key": "name" },
    "stream": {
      "iselection": {
        "input": { "name": "rank" },
        "attr": "rank",  "selector": ">=", "arg": 1
      }
    }
  }
]
'
```

result:
```
a:2:{i:0;a:3:{s:4:"Name";s:0:"";s:5:"Attrs";a:3:{i:1;s:3:"age";i:2;s:3:"job";i:0;s:4:"name";}s:4:"Data";a:1:{s:6:"田中";a:3:{s:4:"name";s:6:"田中";s:3:"age";i:34;s:3:"job";s:15:"デザイナー";}}}i:1;a:3:{s:4:"Data";a:3:{s:6:"清水";a:2:{s:4:"rank";i:78;s:4:"name";s:6:"清水";}s:6:"田中";a:2:{s:4:"name";s:6:"田中";s:4:"rank";i:46;}s:6:"佐藤";a:2:{s:4:"name";s:6:"佐藤";s:4:"rank";i:33;}}s:4:"Name";s:0:"";s:5:"Attrs";a:2:{i:1;s:4:"rank";i:0;s:4:"name";}}}
```


### Relational database term

https://en.wikipedia.org/wiki/Relational_database#Terminology


