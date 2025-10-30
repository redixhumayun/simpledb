window.BENCHMARK_DATA = {
  "lastUpdate": 1761808299366,
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
          "id": "6bd787ff12297e404155f3ed51b50b7f72a94e52",
          "message": "Merge pull request #39 from redixhumayun/benchmarking-ci-comparison\n\nAdd CI benchmark tracking and comparison system",
          "timestamp": "2025-10-19T23:47:57+05:30",
          "tree_id": "7a2800d7b3dc5a5034311ccfba56b1c1fc0bf2d7",
          "url": "https://github.com/redixhumayun/simpledb/commit/6bd787ff12297e404155f3ed51b50b7f72a94e52"
        },
        "date": 1760898294343,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 296,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 5194,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 200251,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 8227265,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 3018853,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 3664971,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 7187264,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 8253426,
            "unit": "ns"
          }
        ]
      },
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
          "id": "610981cca875b7effbbdd19bea927f35bf5a658d",
          "message": "Merge pull request #41 from redixhumayun/benchmarking-ci-comparison\n\nTrack Phase 2 throughput benchmarks in CI",
          "timestamp": "2025-10-24T17:02:54+05:30",
          "tree_id": "3ff5e815e7a1827413ee3abfd29f44124d83fdbd",
          "url": "https://github.com/redixhumayun/simpledb/commit/610981cca875b7effbbdd19bea927f35bf5a658d"
        },
        "date": 1761305622101,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 239,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 2406,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 87004,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 276597,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 2285631,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 1043312,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 1121180,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 1136168,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 1046122,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 5357966,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 2301036,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 2779399,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 5743807,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 5793275,
            "unit": "ns"
          }
        ]
      },
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
          "id": "fcd9d5da06a691a9c61383ebbadc627206706b11",
          "message": "Merge pull request #40 from redixhumayun/buffer-manager-concurrency\n\n- Remove Buffer Pool Global Lock\n- Adds Multi-Threaded Access Pattern Benchmarks\n- Adds Multi-Threaded Contention Benchmarks",
          "timestamp": "2025-10-30T12:40:52+05:30",
          "tree_id": "6e07d1a35aa2c40f0303adae264aead58783f7e8",
          "url": "https://github.com/redixhumayun/simpledb/commit/fcd9d5da06a691a9c61383ebbadc627206706b11"
        },
        "date": 1761808298920,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 516,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 4770,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 135037,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 568343,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 962640,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 4750101,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 5702787,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 2169278,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 2686238,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 2397523,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 3557744,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 2397498,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 3634692,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 2270836,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 2594828,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 11160054,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 30022582,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 60880328,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 5552500,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 5095309,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 6302255,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 2693847,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 3090691,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 6020368,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 6121118,
            "unit": "ns"
          }
        ]
      }
    ]
  }
}