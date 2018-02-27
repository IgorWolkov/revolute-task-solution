
Start the first node of cluster with following parameters:

web host:
```
webAppHost=localhost
```

web port:
```
webAppPort=8080
```

akka cluster seed nodes (this value should be the same for all instances):
```
clusterSeedNodes="akka://ClusterSystem@127.0.0.1:2551"
``` 

akka cluster node host:
```
clusterNodeHost=localhost 
```

akka cluster node port:
```
clusterNodePort=2551
```

lock retries delay in seconds (this value should be the same for all instances):
``` 
clusterRetriesDelay=1
```

number of max attempts to get lock (this value should be the same for all instances):
```
clusterMaxRetries=10 - this value should be the same for all instances
```

timeout for getting response in seconds (this value should be the same for all instances):
```
clusterTimeout=30 run - this value should be the same for all instances
```

Run the first node

```
sbt> sbt -DwebAppHost=localhost -DwebAppPort=8080 -DclusterSeedNodes="akka://ClusterSystem@127.0.0.1:2551" -DclusterNodeHost=localhost -DclusterNodePort=2551 -DclusterRetriesDelay=1 -DclusterMaxRetries=10 -DclusterTimeout=30 run

```

Now cluster is empty. Try to get information for account #`42`
```
GET localhost:8080/v1/accounts/42
```

Response example:
```json
{  
   "RecordsAndBalanceFailed":{  
      "accountId":42,
      "cause":"UnableToRead(8515022145583946982,42,NotFound(8515022145583946982,42))"
   }
}
```

Initiate account #`42`

```
PUT localhost:4400/v1/accounts
```
body:
```json
{  
   "accountId":"42",
   "amount":"100",
   "description":"New account #42"
}
```

Response example:
```json
{  
   "AccountInitiationComplete":{  
      "accountInitiation":{  
         "accountId":42,
         "amount":100,
         "description":"New account #42"
      }
   }
}
```

Add the next #84 account
```
PUT localhost:4400/v1/accounts
```
body:
```json
{  
   "accountId":"84",
   "amount":"100",
   "description":"New account #84"
}
```

Response example:
```json
{  
   "AccountInitiationComplete":{  
      "accountInitiation":{  
         "accountId":84,
         "amount":100,
         "description":"New account #84"
      }
   }
}
```

Get the information about #42 account
```
GET localhost:8080/v1/accounts/42
```

Response example:
```json
{
    "RecordsAndBalance": {
        "accountId": 42,
        "balance": {
            "Balance": {
                "accountId": 42,
                "balance": 100
            }
        },
        "records": [
            {
                "id": 8749751484914314558,
                "accountId": 42,
                "amount": 100,
                "associatedWith": null,
                "description": "New account #42",
                "type": {
                    "Committed": {}
                }
            }
        ]
    }
}
```

Get the information about #84 account
```
GET localhost:8080/v1/accounts/84
```

Response example:
```json
{
    "RecordsAndBalance": {
        "accountId": 84,
        "balance": {
            "Balance": {
                "accountId": 84,
                "balance": 100
            }
        },
        "records": [
            {
                "id": 340719269032724354,
                "accountId": 84,
                "amount": 100,
                "associatedWith": null,
                "description": "New account #84",
                "type": {
                    "Committed": {}
                }
            }
        ]
    }
}
```


Transfer 20 coins from #42 to #84
```
POST localhost:8080/v1/accounts/transfer
```
body:
```json
{
    "from": "42",
    "to": "84",
    "amount": "20",
    "description": "#42 >-- 20 --> #84"
}
```

Response example:
```json
{
    "BankTransferComplete": {
        "bankTransfer": {
            "from": 42,
            "to": 84,
            "amount": 20,
            "description": "#42 >-- 20 --> #84"
        }
    }
}
```

Get the information about #42 account. The balance was changed.
```
GET localhost:8080/v1/accounts/42
```

Response example:
```json
{
    "RecordsAndBalance": {
        "accountId": 42,
        "balance": {
            "Balance": {
                "accountId": 42,
                "balance": 80
            }
        },
        "records": [
            {
                "id": 8749751484914314558,
                "accountId": 42,
                "amount": 100,
                "associatedWith": null,
                "description": "New account #42",
                "type": {
                    "Committed": {}
                }
            },
            {
                "id": 5257846807599146239,
                "accountId": 42,
                "amount": -20,
                "associatedWith": 84,
                "description": "#42 >-- 20 --> #84",
                "type": {
                    "Committed": {}
                }
            }
        ]
    }
}
```

Get the information about #84 account. The balance was changed.
```
GET localhost:8080/v1/accounts/84
```

Response example:
```json
{
    "RecordsAndBalance": {
        "accountId": 84,
        "balance": {
            "Balance": {
                "accountId": 84,
                "balance": 120
            }
        },
        "records": [
            {
                "id": 340719269032724354,
                "accountId": 84,
                "amount": 100,
                "associatedWith": null,
                "description": "New account #84",
                "type": {
                    "Committed": {}
                }
            },
            {
                "id": 5257846807599146239,
                "accountId": 42,
                "amount": 20,
                "associatedWith": 84,
                "description": "#42 >-- 20 --> #84",
                "type": {
                    "Committed": {}
                }
            }
        ]
    }
}
```

Start the second node with

web port
```
webAppPort=8081
```

and akka cluster node port
```
clusterNodePort=2552
```

Run the second node with

```
sbt -DwebAppHost=localhost -DwebAppPort=8081 -DclusterSeedNodes="akka://ClusterSystem@127.0.0.1:2551" -DclusterNodeHost=localhost -DclusterNodePort=2552 -DclusterRetriesDelay=1 -DclusterMaxRetries=10 -DclusterTimeout=30 run

```

Cluster will be rebalanced and all data will be spreaded across 
the two nodes with replication factor 2.

Now let us send the requests for the second node.

Revert the previous transaction of 20 coins from #42 to #84.
To revert transaction you should use transaction id which is 
different for each request. Here I am using `5257846807599146239`.
Also you should provide the original bank transfer.

```
POST localhost:8081/v1/accounts/transfer/5257846807599146239
```
body:
```json
{
    "from": "42",
    "to": "84",
    "amount": "20",
    "description": "#42 >-- 20 --> #84"
}
```

Response example:
```json
{
    "BankTransferReversionComplete": {
        "transactionId": 5257846807599146239,
        "bankTransfer": {
            "from": 42,
            "to": 84,
            "amount": 20,
            "description": "#42 >-- 20 --> #84"
        }
    }
}
```


Get the information about #42 account. The balance was changed.
```
GET localhost:8081/v1/accounts/42
```

Response example:
```json
{
    "RecordsAndBalance": {
        "accountId": 42,
        "balance": {
            "Balance": {
                "accountId": 42,
                "balance": 100
            }
        },
        "records": [
            {
                "id": 8749751484914314558,
                "accountId": 42,
                "amount": 100,
                "associatedWith": null,
                "description": "New account #42",
                "type": {
                    "Committed": {}
                }
            },
            {
                "id": 5257846807599146239,
                "accountId": 42,
                "amount": -20,
                "associatedWith": 84,
                "description": "#42 >-- 20 --> #84",
                "type": {
                    "Committed": {}
                }
            },
            {
                "id": 5257846807599146239,
                "accountId": 42,
                "amount": -20,
                "associatedWith": 84,
                "description": "#42 >-- 20 --> #84",
                "type": {
                    "Reverted": {}
                }
            }
        ]
    }
}
```

Get the information about #84 account. The balance was changed.
```
GET localhost:8081/v1/accounts/84
```

Response example:
```json
{
    "RecordsAndBalance": {
        "accountId": 84,
        "balance": {
            "Balance": {
                "accountId": 84,
                "balance": 100
            }
        },
        "records": [
            {
                "id": 340719269032724354,
                "accountId": 84,
                "amount": 100,
                "associatedWith": null,
                "description": "New account #84",
                "type": {
                    "Committed": {}
                }
            },
            {
                "id": 5257846807599146239,
                "accountId": 42,
                "amount": 20,
                "associatedWith": 84,
                "description": "#42 >-- 20 --> #84",
                "type": {
                    "Committed": {}
                }
            },
            {
                "id": 5257846807599146239,
                "accountId": 42,
                "amount": 20,
                "associatedWith": 84,
                "description": "#42 >-- 20 --> #84",
                "type": {
                    "Reverted": {}
                }
            }
        ]
    }
}
```

**Finally, turn of the first node. Cluster will recover lost data from the recovery copies, 
and the second node will continue to serve the requests.**




