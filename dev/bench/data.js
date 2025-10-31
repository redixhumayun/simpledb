window.BENCHMARK_DATA = {
  "lastUpdate": 1761881326030,
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
          "id": "3e38f2e63216ba17f602d0fb6321209320d2a543",
          "message": "Merge pull request #43 from redixhumayun/feature/benchmarking-cli-execution\n\nThis PR splits up the benchmarks into smaller execution units so that it's easier to run from the CLI by specifying the name of a benchmark and filtering on that name. However, it's still not isolated to the level where it will be useful for performance profiling. That work still remains to be done.",
          "timestamp": "2025-10-30T15:45:19+05:30",
          "tree_id": "b7afed612a67b4fc1c12b694f915b70246557f92",
          "url": "https://github.com/redixhumayun/simpledb/commit/3e38f2e63216ba17f602d0fb6321209320d2a543"
        },
        "date": 1761819381568,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 1026,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 4900,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 100541,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 564983,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 987249,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 4725089,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 7241061,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 2138498,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 3425880,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 2296574,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 4429110,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 2364837,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 4541708,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 2164232,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 3629334,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 15473801,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 34268950,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 70461393,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 3514383,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 6675307,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 6072945,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 2518604,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 3141321,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 6244337,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 6452609,
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
          "id": "651ee5b6f4d680519673a1e0f9d3dc739c4cccb0",
          "message": "Merge pull request #44 from redixhumayun/self-hosted-ci-runner\n\nSelf Hosted Runner",
          "timestamp": "2025-10-30T15:58:28+05:30",
          "tree_id": "2d1e678f28c7008a753194b28e17d3a52061540e",
          "url": "https://github.com/redixhumayun/simpledb/commit/651ee5b6f4d680519673a1e0f9d3dc739c4cccb0"
        },
        "date": 1761820288969,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 3387,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 23686,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 5007299,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 1067539,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 1810383,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 6566094,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 8995748,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 3039280,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 5639949,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 3328627,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 7199926,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 3344714,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 6835645,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 3028877,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 4151832,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 14025351,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 28396015,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 68385196,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 3054783,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 7556117,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 103244762,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 40206205,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 46646873,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 92233256,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 102881608,
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
          "id": "b46be4934a517231606819358341385092e2e8d9",
          "message": "Merge pull request #45 from redixhumayun/feature/benchmarking-json-output\n\nFixed warning about unused variable",
          "timestamp": "2025-10-30T23:03:42+05:30",
          "tree_id": "d8703cafe32d21ca312eae63a9aa8570f1d4d9d2",
          "url": "https://github.com/redixhumayun/simpledb/commit/b46be4934a517231606819358341385092e2e8d9"
        },
        "date": 1761845782132,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 804,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 6394,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 5001190,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 728471,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 994936,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 6156237,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 6518947,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 2808624,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 2927033,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 3020071,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 3846031,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 3029695,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 3910044,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 2858864,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 2746738,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 13678297,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 28486093,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 67092203,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 4645699,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 8428824,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 103275364,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 40063840,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 46757158,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 92283095,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 103089074,
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
            "email": "redixhumayun@gmail.com",
            "name": "Zaid Humayun",
            "username": "redixhumayun"
          },
          "distinct": true,
          "id": "d2f9c3c912def69d42210596cab362809b4b3852",
          "message": "updated AGENTS.md file",
          "timestamp": "2025-10-31T08:55:09+05:30",
          "tree_id": "fdfd976e127d171785dc007cc73991823274fea1",
          "url": "https://github.com/redixhumayun/simpledb/commit/d2f9c3c912def69d42210596cab362809b4b3852"
        },
        "date": 1761881324692,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 1337,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 6415,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 4998280,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 728992,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 1006795,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 6133120,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 6198756,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 2911287,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 3008437,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 3015747,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 3801282,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 3035202,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 3904685,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 2848456,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 3031995,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 13599154,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 27928823,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 68475044,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 5430964,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 8006983,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 143812308,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 40123929,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 46517533,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 92233319,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 110698807,
            "unit": "ns"
          }
        ]
      }
    ]
  }
}