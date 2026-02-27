window.BENCHMARK_DATA = {
  "lastUpdate": 1772177008408,
  "repoUrl": "https://github.com/redixhumayun/simpledb",
  "entries": {
    "SimpleDB Benchmarks": [
      {
        "commit": {
          "author": {
            "email": "redixhumayun@gmail.com",
            "name": "Zaid Humayun",
            "username": "redixhumayun"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7e9c731c1fce308457e3c4b8984e8e92fc974033",
          "message": "Merge pull request #80 from redixhumayun/feature/criterion\n\nfeat: replace custom benchmark harness with Criterion, add Clap to CLI",
          "timestamp": "2026-02-26T22:57:23-08:00",
          "tree_id": "0deedc5125b34994081abbed91836aa241d50cd1",
          "url": "https://github.com/redixhumayun/simpledb/commit/7e9c731c1fce308457e3c4b8984e8e92fc974033"
        },
        "date": 1772177007506,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Phase1/Core Latency/Pin/Unpin (hit)",
            "value": 1124,
            "unit": "ns"
          },
          {
            "name": "Phase1/Core Latency/Cold Pin (miss)",
            "value": 21122,
            "unit": "ns"
          },
          {
            "name": "Phase1/Core Latency/Dirty Eviction",
            "value": 2793,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns ST/Sequential Scan (120 blocks)",
            "value": 4410009,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns ST/Repeated Access (1000 ops)",
            "value": 1171209,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns ST/Random (K=10, 500 ops)",
            "value": 583619,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns ST/Random (K=50, 500 ops)",
            "value": 32096222,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns ST/Random (K=100, 500 ops)",
            "value": 37267587,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns ST/Zipfian (80/20, 500 ops)",
            "value": 10445470,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns MT/Seq Scan MT x2 (120 blocks)",
            "value": 10170725,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns MT/Seq Scan MT x4 (120 blocks)",
            "value": 8679230,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns MT/Seq Scan MT x8 (120 blocks)",
            "value": 8717508,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns MT/Seq Scan MT x16 (120 blocks)",
            "value": 10400719,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns MT/Seq Scan MT x32 (120 blocks)",
            "value": 31262855,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns MT/Seq Scan MT x64 (120 blocks)",
            "value": 62083330,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns MT/Seq Scan MT x128 (120 blocks)",
            "value": 58374824,
            "unit": "ns"
          },
          {
            "name": "Phase2/Access Patterns MT/Seq Scan MT x256 (120 blocks)",
            "value": 48139970,
            "unit": "ns"
          },
          {
            "name": "Phase3/Pool Scaling/Random Access/8",
            "value": 39252703,
            "unit": "ns"
          },
          {
            "name": "Phase3/Pool Scaling/Random Access/16",
            "value": 34392939,
            "unit": "ns"
          },
          {
            "name": "Phase3/Pool Scaling/Random Access/32",
            "value": 29372038,
            "unit": "ns"
          },
          {
            "name": "Phase3/Pool Scaling/Random Access/64",
            "value": 13822193,
            "unit": "ns"
          },
          {
            "name": "Phase3/Pool Scaling/Random Access/128",
            "value": 588758,
            "unit": "ns"
          },
          {
            "name": "Phase3/Pool Scaling/Random Access/256",
            "value": 591579,
            "unit": "ns"
          },
          {
            "name": "Phase5/Concurrent Pin/Concurrent (1 threads, 10000 ops)",
            "value": 11762993,
            "unit": "ns"
          },
          {
            "name": "Phase5/Concurrent Pin/Concurrent (2 threads, 5000 ops)",
            "value": 11515460,
            "unit": "ns"
          },
          {
            "name": "Phase5/Concurrent Pin/Concurrent (4 threads, 2500 ops)",
            "value": 24465179,
            "unit": "ns"
          },
          {
            "name": "Phase5/Concurrent Pin/Concurrent (8 threads, 1250 ops)",
            "value": 51313480,
            "unit": "ns"
          },
          {
            "name": "Phase5/Concurrent Pin/Concurrent (16 threads, 625 ops)",
            "value": 22180553,
            "unit": "ns"
          },
          {
            "name": "Phase5/Concurrent Pin/Concurrent (32 threads, 312 ops)",
            "value": 21914766,
            "unit": "ns"
          },
          {
            "name": "Phase5/Concurrent Pin/Concurrent (64 threads, 156 ops)",
            "value": 22060947,
            "unit": "ns"
          },
          {
            "name": "Phase5/Concurrent Pin/Concurrent (128 threads, 78 ops)",
            "value": 22126267,
            "unit": "ns"
          },
          {
            "name": "Phase5/Concurrent Pin/Concurrent (256 threads, 39 ops)",
            "value": 22781962,
            "unit": "ns"
          },
          {
            "name": "Phase5/Hotset Contention/Concurrent Hotset (1 threads, K=4, 10000 ops)",
            "value": 12020554,
            "unit": "ns"
          },
          {
            "name": "Phase5/Hotset Contention/Concurrent Hotset (2 threads, K=4, 5000 ops)",
            "value": 10930411,
            "unit": "ns"
          },
          {
            "name": "Phase5/Hotset Contention/Concurrent Hotset (4 threads, K=4, 2500 ops)",
            "value": 18188597,
            "unit": "ns"
          },
          {
            "name": "Phase5/Hotset Contention/Concurrent Hotset (8 threads, K=4, 1250 ops)",
            "value": 21568727,
            "unit": "ns"
          },
          {
            "name": "Phase5/Hotset Contention/Concurrent Hotset (16 threads, K=4, 625 ops)",
            "value": 19663453,
            "unit": "ns"
          },
          {
            "name": "Phase5/Hotset Contention/Concurrent Hotset (32 threads, K=4, 312 ops)",
            "value": 19550167,
            "unit": "ns"
          },
          {
            "name": "Phase5/Hotset Contention/Concurrent Hotset (64 threads, K=4, 156 ops)",
            "value": 19626704,
            "unit": "ns"
          },
          {
            "name": "Phase5/Hotset Contention/Concurrent Hotset (128 threads, K=4, 78 ops)",
            "value": 19838498,
            "unit": "ns"
          },
          {
            "name": "Phase5/Hotset Contention/Concurrent Hotset (256 threads, K=4, 39 ops)",
            "value": 20329393,
            "unit": "ns"
          },
          {
            "name": "Phase1/IO Throughput/Sequential Read (1000 ops)",
            "value": 20919596,
            "unit": "ns"
          },
          {
            "name": "Phase1/IO Throughput/Sequential Write (1000 ops)",
            "value": 22142386,
            "unit": "ns"
          },
          {
            "name": "Phase1/IO Throughput/Random Read (1000 ops)",
            "value": 86105895,
            "unit": "ns"
          },
          {
            "name": "Phase1/IO Throughput/Random Write (1000 ops)",
            "value": 22116516,
            "unit": "ns"
          },
          {
            "name": "Phase1/Queue Depth/Sequential Read QD/1",
            "value": 20997184,
            "unit": "ns"
          },
          {
            "name": "Phase1/Queue Depth/Random Read QD/1",
            "value": 85414965,
            "unit": "ns"
          },
          {
            "name": "Phase1/Queue Depth/Multi-stream Scan QD/1",
            "value": 26870073,
            "unit": "ns"
          },
          {
            "name": "Phase1/Queue Depth/Sequential Read QD/4",
            "value": 10540039,
            "unit": "ns"
          },
          {
            "name": "Phase1/Queue Depth/Random Read QD/4",
            "value": 26163313,
            "unit": "ns"
          },
          {
            "name": "Phase1/Queue Depth/Multi-stream Scan QD/4",
            "value": 12965426,
            "unit": "ns"
          },
          {
            "name": "Phase1/Queue Depth/Sequential Read QD/16",
            "value": 8176475,
            "unit": "ns"
          },
          {
            "name": "Phase1/Queue Depth/Random Read QD/16",
            "value": 10018931,
            "unit": "ns"
          },
          {
            "name": "Phase1/Queue Depth/Multi-stream Scan QD/16",
            "value": 8970695,
            "unit": "ns"
          },
          {
            "name": "Phase1/Queue Depth/Sequential Read QD/32",
            "value": 5638199,
            "unit": "ns"
          },
          {
            "name": "Phase1/Queue Depth/Random Read QD/32",
            "value": 8052391,
            "unit": "ns"
          },
          {
            "name": "Phase1/Queue Depth/Multi-stream Scan QD/32",
            "value": 6850863,
            "unit": "ns"
          },
          {
            "name": "Phase2/WAL/append no-fsync",
            "value": 229763857,
            "unit": "ns"
          },
          {
            "name": "Phase2/WAL/append immediate-fsync",
            "value": 872041960,
            "unit": "ns"
          },
          {
            "name": "Phase2/WAL/group commit/10",
            "value": 1096249601,
            "unit": "ns"
          },
          {
            "name": "Phase2/WAL/group commit/50",
            "value": 413140063,
            "unit": "ns"
          },
          {
            "name": "Phase2/WAL/group commit/100",
            "value": 316997439,
            "unit": "ns"
          },
          {
            "name": "Phase3/Mixed R/W/Mixed 70/30/no-fsync",
            "value": 74611551,
            "unit": "ns"
          },
          {
            "name": "Phase3/Mixed R/W/Mixed 70/30/immediate-fsync",
            "value": 1431670531,
            "unit": "ns"
          },
          {
            "name": "Phase3/Mixed R/W/Mixed 70/30/group-10",
            "value": 226831408,
            "unit": "ns"
          },
          {
            "name": "Phase3/Mixed R/W/Mixed 50/50/no-fsync",
            "value": 98025704,
            "unit": "ns"
          },
          {
            "name": "Phase3/Mixed R/W/Mixed 50/50/immediate-fsync",
            "value": 2276761813,
            "unit": "ns"
          },
          {
            "name": "Phase3/Mixed R/W/Mixed 50/50/group-10",
            "value": 345293722,
            "unit": "ns"
          },
          {
            "name": "Phase3/Mixed R/W/Mixed 10/90/no-fsync",
            "value": 141826631,
            "unit": "ns"
          },
          {
            "name": "Phase3/Mixed R/W/Mixed 10/90/immediate-fsync",
            "value": 4115968757,
            "unit": "ns"
          },
          {
            "name": "Phase3/Mixed R/W/Mixed 10/90/group-10",
            "value": 569242120,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Shared 2T no-fsync",
            "value": 34597093,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Sharded 2T no-fsync",
            "value": 38005915,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Shared 2T group-10",
            "value": 115775663,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Sharded 2T group-10",
            "value": 112155045,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Shared 4T no-fsync",
            "value": 82470001,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Sharded 4T no-fsync",
            "value": 77903075,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Shared 4T group-10",
            "value": 250262631,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Sharded 4T group-10",
            "value": 236597582,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Shared 8T no-fsync",
            "value": 164315243,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Sharded 8T no-fsync",
            "value": 157262318,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Shared 8T group-10",
            "value": 489721818,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Sharded 8T group-10",
            "value": 494343698,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Shared 16T no-fsync",
            "value": 323517208,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Sharded 16T no-fsync",
            "value": 306279836,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Shared 16T group-10",
            "value": 985206741,
            "unit": "ns"
          },
          {
            "name": "Phase4/Concurrent IO/Sharded 16T group-10",
            "value": 950243994,
            "unit": "ns"
          },
          {
            "name": "Phase5/Durability/Random Write durability immediate-fsync data-nosync",
            "value": 967866981,
            "unit": "ns"
          },
          {
            "name": "Phase5/Durability/Random Write durability immediate-fsync data-fsync",
            "value": 972894895,
            "unit": "ns"
          },
          {
            "name": "Phase7/Cache Adverse/One-pass Seq Scan (1000 blocks)",
            "value": 20981070,
            "unit": "ns"
          },
          {
            "name": "Phase7/Cache Adverse/Low-locality Rand Read (1000 blocks)",
            "value": 85586931,
            "unit": "ns"
          },
          {
            "name": "Phase7/Cache Adverse/Multi-stream Scan (1000 blocks)",
            "value": 52341022,
            "unit": "ns"
          },
          {
            "name": "Phase8/Cache Evict/One-pass Seq Scan+Evict (1000 blocks)",
            "value": 21257513,
            "unit": "ns"
          },
          {
            "name": "Phase8/Cache Evict/Low-locality Rand Read+Evict (1000 blocks)",
            "value": 87339801,
            "unit": "ns"
          },
          {
            "name": "Phase8/Cache Evict/Multi-stream Scan+Evict (1000 blocks)",
            "value": 57239066,
            "unit": "ns"
          },
          {
            "name": "DML/Insert/INSERT single record",
            "value": 37806351,
            "unit": "ns"
          },
          {
            "name": "SELECT/table scan",
            "value": 8658109,
            "unit": "ns"
          },
          {
            "name": "SELECT/full scan count",
            "value": 9523729,
            "unit": "ns"
          },
          {
            "name": "DML/Update/UPDATE single record",
            "value": 37133765,
            "unit": "ns"
          },
          {
            "name": "DML/Delete/DELETE single record",
            "value": 37002693,
            "unit": "ns"
          }
        ]
      }
    ]
  }
}