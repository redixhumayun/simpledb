window.BENCHMARK_DATA = {
  "lastUpdate": 1771296945770,
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
          "id": "84fd53944e2d066e3c3840f63911a4c155bd135d",
          "message": "Added details of profiling plans after reading Sled's performance guide",
          "timestamp": "2025-10-31T12:13:01+05:30",
          "tree_id": "f707da1c67fd9269782fa03ea1cc485354f3dbfc",
          "url": "https://github.com/redixhumayun/simpledb/commit/84fd53944e2d066e3c3840f63911a4c155bd135d"
        },
        "date": 1761893160529,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 934,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 7119,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 5378518,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 731263,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 1050256,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 6357097,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 5113389,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 2971119,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 3087341,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 3411044,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 4366973,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 3434557,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 4134378,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 3050726,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 3237901,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 14272886,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 28532125,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 66694735,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 3832477,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 8188723,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 102398020,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 40310776,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 46111017,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 90888932,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 101425549,
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
          "id": "1d37faf6877ea985f3151d33bf36f6e00bf625a2",
          "message": "Updated README with exact commands used to get IO perf numbers",
          "timestamp": "2025-10-31T15:45:12+05:30",
          "tree_id": "0e26d37ae36f326adc037c99aa238c0ecc387a74",
          "url": "https://github.com/redixhumayun/simpledb/commit/1d37faf6877ea985f3151d33bf36f6e00bf625a2"
        },
        "date": 1761905882976,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 799,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 6375,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 5000041,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 728910,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 994085,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 6120621,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 6464571,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 2777649,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 2880633,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 3008963,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 3823404,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 3008674,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 3896622,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 2839742,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 3117880,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 13607121,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 27907740,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 67271564,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 3795756,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 8031937,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 101443053,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 40307321,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 41763561,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 90629055,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 101396178,
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
          "id": "8a6d0134f99adb7f8e58f96c3d21cb3633850853",
          "message": "Merge pull request #47 from redixhumayun/feature/benchmarking-refactor\n\nUpdate Buffer Pool Benchmarks",
          "timestamp": "2025-10-31T16:11:57+05:30",
          "tree_id": "786ddb1cb003fcc49061c87502dc4e5b67e88543",
          "url": "https://github.com/redixhumayun/simpledb/commit/8a6d0134f99adb7f8e58f96c3d21cb3633850853"
        },
        "date": 1761907473348,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 818,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 6258,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 4999034,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 728416,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 982517,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 6131880,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 6313155,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 2858915,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 3020910,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 3110188,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 2955466,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 3864644,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 3911962,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 2812405,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 3029047,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 13695150,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 27844585,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 68964383,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 4173009,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 8347144,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 102982897,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 41707505,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 45260630,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 91938491,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 103033547,
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
          "id": "f6a7707ef3be9132eebcec29e12cf4524e069448",
          "message": "Merge pull request #49 from redixhumayun/multithreading_audit\n\nSimplify metadata synchronization and document Arc usage",
          "timestamp": "2025-11-01T17:27:10+05:30",
          "tree_id": "62ead76f3fb6a914275331aa29fc6853aed8bd7b",
          "url": "https://github.com/redixhumayun/simpledb/commit/f6a7707ef3be9132eebcec29e12cf4524e069448"
        },
        "date": 1761998366994,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 818,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 6467,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 5006418,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 739101,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 983785,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 6170918,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 6006656,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 2828487,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 3000356,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 3033802,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 2982549,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 3802607,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 3856788,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 2899634,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 3127570,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 13706895,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 28526065,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 70549648,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 6388094,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 8470261,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 22064853,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 5402288,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 5502349,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 21306089,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 22054922,
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
          "id": "9d593ff9ade4ce26efba3af19611af2276416612",
          "message": "Merge pull request #48 from redixhumayun/benches/io-benchmarks\n\nThis PR:\n\n* adds IO benchmarks\n* the results of `fio` benchmarks to a README.md file\n* cleanup of buffer pool benchmarks",
          "timestamp": "2025-11-04T11:44:35+05:30",
          "tree_id": "e30a324920cfa63ac2cf94c5c4e77f961054c3ef",
          "url": "https://github.com/redixhumayun/simpledb/commit/9d593ff9ade4ce26efba3af19611af2276416612"
        },
        "date": 1762237190221,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 819,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 6501,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 5000674,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 723902,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 982621,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 6083289,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 6345102,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 2759221,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 3013765,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 3010710,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 2878151,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 3841800,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 3876282,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 2836105,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 2977458,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 13559008,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 27950590,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 66825569,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 5146389,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 8205043,
            "unit": "ns"
          },
          {
            "name": "Sequential Read (1000 blocks)",
            "value": 2513871,
            "unit": "ns"
          },
          {
            "name": "Sequential Write (1000 blocks)",
            "value": 2672681,
            "unit": "ns"
          },
          {
            "name": "Random Read (K=1000, 1000 ops)",
            "value": 2506965,
            "unit": "ns"
          },
          {
            "name": "Random Write (K=1000, 1000 ops)",
            "value": 2776445,
            "unit": "ns"
          },
          {
            "name": "WAL append (no fsync)",
            "value": 133046332,
            "unit": "ns"
          },
          {
            "name": "WAL append + immediate fsync",
            "value": 512066990,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=10)",
            "value": 631233345,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=50)",
            "value": 231117343,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=100)",
            "value": 178085963,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W no-fsync",
            "value": 20071827,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W immediate-fsync",
            "value": 847398823,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W group-10",
            "value": 88034597,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W no-fsync",
            "value": 30977115,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W immediate-fsync",
            "value": 1275379773,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W group-10",
            "value": 163061725,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W no-fsync",
            "value": 58022494,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W immediate-fsync",
            "value": 2364765369,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W group-10",
            "value": 295070082,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T no-fsync",
            "value": 7114112,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T no-fsync",
            "value": 6981071,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T group-10",
            "value": 36974024,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T group-10",
            "value": 36922292,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T no-fsync",
            "value": 14990887,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T no-fsync",
            "value": 14998806,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T group-10",
            "value": 75041406,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T group-10",
            "value": 75103495,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T no-fsync",
            "value": 30990028,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T no-fsync",
            "value": 31076332,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T group-10",
            "value": 151051569,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T group-10",
            "value": 150998669,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T no-fsync",
            "value": 61892408,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T no-fsync",
            "value": 62974859,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T group-10",
            "value": 301063799,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T group-10",
            "value": 303388378,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-nosync",
            "value": 5156888353,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-fsync",
            "value": 10041669541,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 22264368,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 5399642,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 5506370,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 21216372,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 22102169,
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
          "id": "5943f4c485771196b8f705e8bcbb32fff59808eb",
          "message": "removed Batched pattern from DataSyncPolicy",
          "timestamp": "2025-11-04T15:46:11+05:30",
          "tree_id": "bc3a133c271b2b8219e3a226315cdfee347ccc0a",
          "url": "https://github.com/redixhumayun/simpledb/commit/5943f4c485771196b8f705e8bcbb32fff59808eb"
        },
        "date": 1762251695613,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 820,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 6800,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 5067390,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 720817,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 995018,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 6096722,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 5840787,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 2790228,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 2976251,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 2991940,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 2978292,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 3871488,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 3868896,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 2768999,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 2942116,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 13408835,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 27732737,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 66738814,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 3419883,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 7892920,
            "unit": "ns"
          },
          {
            "name": "Sequential Read (1000 blocks)",
            "value": 2651160,
            "unit": "ns"
          },
          {
            "name": "Sequential Write (1000 blocks)",
            "value": 2793649,
            "unit": "ns"
          },
          {
            "name": "Random Read (K=1000, 1000 ops)",
            "value": 2642561,
            "unit": "ns"
          },
          {
            "name": "Random Write (K=1000, 1000 ops)",
            "value": 2785231,
            "unit": "ns"
          },
          {
            "name": "WAL append (no fsync)",
            "value": 130058927,
            "unit": "ns"
          },
          {
            "name": "WAL append + immediate fsync",
            "value": 517123293,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=10)",
            "value": 628198246,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=50)",
            "value": 228016906,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=100)",
            "value": 183089943,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W no-fsync",
            "value": 17941351,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W immediate-fsync",
            "value": 820318694,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W group-10",
            "value": 94134786,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W no-fsync",
            "value": 32961073,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W immediate-fsync",
            "value": 1377537351,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W group-10",
            "value": 156072773,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W no-fsync",
            "value": 57987567,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W immediate-fsync",
            "value": 2256943555,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W group-10",
            "value": 283126675,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T no-fsync",
            "value": 7043445,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T no-fsync",
            "value": 7100483,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T group-10",
            "value": 37097652,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T group-10",
            "value": 36997973,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T no-fsync",
            "value": 15057989,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T no-fsync",
            "value": 15020957,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T group-10",
            "value": 75003009,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T group-10",
            "value": 75988247,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T no-fsync",
            "value": 30922611,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T no-fsync",
            "value": 30947729,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T group-10",
            "value": 151077967,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T group-10",
            "value": 150144376,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T no-fsync",
            "value": 61907307,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T no-fsync",
            "value": 62040675,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T group-10",
            "value": 300169354,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T group-10",
            "value": 302133332,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-nosync",
            "value": 5163114002,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-fsync",
            "value": 10157032551,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 22260431,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 5397964,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 5504130,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 21659246,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 22073420,
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
          "id": "785f2a20985a96c31e2e152ea9b93c06a84836d9",
          "message": "Merge pull request #51 from redixhumayun/feature/multithreading-tests\n\nFixes the flaky test_transaction_isolation_with_concurrent_writes test",
          "timestamp": "2025-11-11T16:21:27+05:30",
          "tree_id": "42ca0b1bc2cce08fe3ec48ae8bb96c122c57cc96",
          "url": "https://github.com/redixhumayun/simpledb/commit/785f2a20985a96c31e2e152ea9b93c06a84836d9"
        },
        "date": 1762858595566,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 815,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 6882,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 5002574,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 738731,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 972671,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 6099064,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 6246010,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 2831565,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 3015995,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 3031464,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 2858260,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 3797623,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 3926194,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 2862755,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 3083569,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 13527764,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 28257509,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 67137460,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 3017499,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 7986014,
            "unit": "ns"
          },
          {
            "name": "Sequential Read (1000 blocks)",
            "value": 2572953,
            "unit": "ns"
          },
          {
            "name": "Sequential Write (1000 blocks)",
            "value": 2721274,
            "unit": "ns"
          },
          {
            "name": "Random Read (K=1000, 1000 ops)",
            "value": 2570608,
            "unit": "ns"
          },
          {
            "name": "Random Write (K=1000, 1000 ops)",
            "value": 2705527,
            "unit": "ns"
          },
          {
            "name": "WAL append (no fsync)",
            "value": 130975501,
            "unit": "ns"
          },
          {
            "name": "WAL append + immediate fsync",
            "value": 515237173,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=10)",
            "value": 630214175,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=50)",
            "value": 228015250,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=100)",
            "value": 179045968,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W no-fsync",
            "value": 18050382,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W immediate-fsync",
            "value": 722158795,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W group-10",
            "value": 88003065,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W no-fsync",
            "value": 32009310,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W immediate-fsync",
            "value": 1361421793,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W group-10",
            "value": 150016914,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W no-fsync",
            "value": 60043293,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W immediate-fsync",
            "value": 2330809352,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W group-10",
            "value": 283114246,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T no-fsync",
            "value": 7095968,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T no-fsync",
            "value": 7026170,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T group-10",
            "value": 37043943,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T group-10",
            "value": 36983869,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T no-fsync",
            "value": 15075026,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T no-fsync",
            "value": 14952469,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T group-10",
            "value": 74931671,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T group-10",
            "value": 75070866,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T no-fsync",
            "value": 30934329,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T no-fsync",
            "value": 30930173,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T group-10",
            "value": 152016008,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T group-10",
            "value": 151071895,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T no-fsync",
            "value": 61916536,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T no-fsync",
            "value": 61806142,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T group-10",
            "value": 307055727,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T group-10",
            "value": 299990230,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-nosync",
            "value": 5163227609,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-fsync",
            "value": 10152563820,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 22259426,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 5405960,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 5500980,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 21301413,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 22013170,
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
          "id": "e03299c082cb3b0dbd050298f8fd4f9093491151",
          "message": "Adds Replacement Policies To The Buffer Pool\n\nThis commit adds the following replacement policies to the buffer pool:\n\n* LRU\n* Clock\n* SIEVE\n\nAlso adds additional benchmarks and surrounding scripts to generate summarization tables from benchmark results",
          "timestamp": "2025-11-14T20:06:19+05:30",
          "tree_id": "de2d6f5a2ca1a837635e954a97ec817b1e2031d3",
          "url": "https://github.com/redixhumayun/simpledb/commit/e03299c082cb3b0dbd050298f8fd4f9093491151"
        },
        "date": 1763131302442,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 803,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 3990,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 5003844,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 468291,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 683310,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x16 (120 blocks)",
            "value": 904550,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 847941,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 818993,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x16 (1000 ops)",
            "value": 1141218,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 433071,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 1653285,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 1823965,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 440401,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=10, 500 ops)",
            "value": 573731,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 2064337,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=50, 500 ops)",
            "value": 2978735,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 2429086,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=100, 500 ops)",
            "value": 3446012,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 812391,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 715743,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x16 (80/20, 500 ops)",
            "value": 947045,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 9115678,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 20315135,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 46714369,
            "unit": "ns"
          },
          {
            "name": "Concurrent (16 threads, 1000 ops)",
            "value": 150912162,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 4172551,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 10448702,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (16 threads, K=4, 1000 ops)",
            "value": 22340276,
            "unit": "ns"
          },
          {
            "name": "Sequential Read (1000 blocks)",
            "value": 2891610,
            "unit": "ns"
          },
          {
            "name": "Sequential Write (1000 blocks)",
            "value": 3056037,
            "unit": "ns"
          },
          {
            "name": "Random Read (K=1000, 1000 ops)",
            "value": 2904699,
            "unit": "ns"
          },
          {
            "name": "Random Write (K=1000, 1000 ops)",
            "value": 3036958,
            "unit": "ns"
          },
          {
            "name": "WAL append (no fsync)",
            "value": 129212563,
            "unit": "ns"
          },
          {
            "name": "WAL append + immediate fsync",
            "value": 511129070,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=10)",
            "value": 632197560,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=50)",
            "value": 229029391,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=100)",
            "value": 178099587,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W no-fsync",
            "value": 25807230,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W immediate-fsync",
            "value": 758239948,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W group-10",
            "value": 89007902,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W no-fsync",
            "value": 31967229,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W immediate-fsync",
            "value": 1232410018,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W group-10",
            "value": 171104059,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W no-fsync",
            "value": 57989188,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W immediate-fsync",
            "value": 2326119067,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W group-10",
            "value": 294080929,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T no-fsync",
            "value": 7067499,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T no-fsync",
            "value": 7062092,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T group-10",
            "value": 37942018,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T group-10",
            "value": 37054823,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T no-fsync",
            "value": 15108795,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T no-fsync",
            "value": 15119845,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T group-10",
            "value": 74983316,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T group-10",
            "value": 75134815,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T no-fsync",
            "value": 30975690,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T no-fsync",
            "value": 31277190,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T group-10",
            "value": 151007723,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T group-10",
            "value": 151082454,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T no-fsync",
            "value": 61938627,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T no-fsync",
            "value": 61956894,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T group-10",
            "value": 302066831,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T group-10",
            "value": 301087992,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-nosync",
            "value": 5160853036,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-fsync",
            "value": 10153477791,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 21310868,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 5247441,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 5100730,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 20557699,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 21355890,
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
          "id": "9103270547b1ae3d0b05db4212b4df28fa4e21ef",
          "message": "Merge pull request #56 from redixhumayun/chore/cleanup-compiler-warnings\n\nRemoved compiler directives and refactored code to avoid warnings",
          "timestamp": "2025-11-15T19:40:30+05:30",
          "tree_id": "00e9ba7ecf115b387b05033935ead61f0dd9c591",
          "url": "https://github.com/redixhumayun/simpledb/commit/9103270547b1ae3d0b05db4212b4df28fa4e21ef"
        },
        "date": 1763216196024,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 809,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 4262,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 5250658,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 471379,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 686900,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x16 (120 blocks)",
            "value": 885525,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 872857,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 805839,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x16 (1000 ops)",
            "value": 1150375,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 417744,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 1569017,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 1838016,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 440016,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=10, 500 ops)",
            "value": 587210,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 2056029,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=50, 500 ops)",
            "value": 2992326,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 2360798,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=100, 500 ops)",
            "value": 3422304,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 734153,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 703973,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x16 (80/20, 500 ops)",
            "value": 900304,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 9161337,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 20380123,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 45108605,
            "unit": "ns"
          },
          {
            "name": "Concurrent (16 threads, 1000 ops)",
            "value": 149388521,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 4110086,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 10414205,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (16 threads, K=4, 1000 ops)",
            "value": 22412283,
            "unit": "ns"
          },
          {
            "name": "Sequential Read (1000 blocks)",
            "value": 3083917,
            "unit": "ns"
          },
          {
            "name": "Sequential Write (1000 blocks)",
            "value": 3229516,
            "unit": "ns"
          },
          {
            "name": "Random Read (K=1000, 1000 ops)",
            "value": 3048902,
            "unit": "ns"
          },
          {
            "name": "Random Write (K=1000, 1000 ops)",
            "value": 3235660,
            "unit": "ns"
          },
          {
            "name": "WAL append (no fsync)",
            "value": 129154601,
            "unit": "ns"
          },
          {
            "name": "WAL append + immediate fsync",
            "value": 516227753,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=10)",
            "value": 627275029,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=50)",
            "value": 228064396,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=100)",
            "value": 178053104,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W no-fsync",
            "value": 20048857,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W immediate-fsync",
            "value": 755320001,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W group-10",
            "value": 100024538,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W no-fsync",
            "value": 35483320,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W immediate-fsync",
            "value": 1383457827,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W group-10",
            "value": 163067656,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W no-fsync",
            "value": 57909444,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W immediate-fsync",
            "value": 2254919467,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W group-10",
            "value": 282069280,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T no-fsync",
            "value": 7055366,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T no-fsync",
            "value": 7021690,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T group-10",
            "value": 36949608,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T group-10",
            "value": 37258582,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T no-fsync",
            "value": 15104624,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T no-fsync",
            "value": 15048607,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T group-10",
            "value": 74998325,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T group-10",
            "value": 75102035,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T no-fsync",
            "value": 30966007,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T no-fsync",
            "value": 30940193,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T group-10",
            "value": 152065809,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T group-10",
            "value": 151175475,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T no-fsync",
            "value": 61810821,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T no-fsync",
            "value": 61857356,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T group-10",
            "value": 308099283,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T group-10",
            "value": 300192326,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-nosync",
            "value": 5162878711,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-fsync",
            "value": 10151766995,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 21553135,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 5046019,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 5101193,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 20569994,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 21353328,
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
          "id": "a35001f2fdd3dafbbd9698287bc2ea6bd0959a9e",
          "message": "Merge pull request #58 from redixhumayun/feature/direct-io\n\nUpdated Page Structure Document",
          "timestamp": "2025-11-18T20:44:59+05:30",
          "tree_id": "7d05a082d7ac8dbb372659c6c9596f6e357454ec",
          "url": "https://github.com/redixhumayun/simpledb/commit/a35001f2fdd3dafbbd9698287bc2ea6bd0959a9e"
        },
        "date": 1763479270539,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 821,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 4481,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 5005878,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 475524,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 696068,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x16 (120 blocks)",
            "value": 897564,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 862837,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 823712,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x16 (1000 ops)",
            "value": 1165758,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 432205,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 1640176,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 1812918,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 478360,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=10, 500 ops)",
            "value": 590117,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 2090475,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=50, 500 ops)",
            "value": 2959223,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 2391466,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=100, 500 ops)",
            "value": 3409269,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 797895,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 713369,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x16 (80/20, 500 ops)",
            "value": 1168936,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 9086697,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 20407723,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 45611113,
            "unit": "ns"
          },
          {
            "name": "Concurrent (16 threads, 1000 ops)",
            "value": 144778881,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 3960941,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 10018699,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (16 threads, K=4, 1000 ops)",
            "value": 21323362,
            "unit": "ns"
          },
          {
            "name": "Sequential Read (1000 blocks)",
            "value": 2829468,
            "unit": "ns"
          },
          {
            "name": "Sequential Write (1000 blocks)",
            "value": 2983699,
            "unit": "ns"
          },
          {
            "name": "Random Read (K=1000, 1000 ops)",
            "value": 2835223,
            "unit": "ns"
          },
          {
            "name": "Random Write (K=1000, 1000 ops)",
            "value": 2960059,
            "unit": "ns"
          },
          {
            "name": "WAL append (no fsync)",
            "value": 128181228,
            "unit": "ns"
          },
          {
            "name": "WAL append + immediate fsync",
            "value": 511060509,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=10)",
            "value": 632159656,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=50)",
            "value": 228331222,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=100)",
            "value": 178124184,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W no-fsync",
            "value": 17123957,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W immediate-fsync",
            "value": 810311994,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W group-10",
            "value": 93071225,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W no-fsync",
            "value": 35023001,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W immediate-fsync",
            "value": 1277325123,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W group-10",
            "value": 150013154,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W no-fsync",
            "value": 58003319,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W immediate-fsync",
            "value": 2339752039,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W group-10",
            "value": 292034009,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T no-fsync",
            "value": 7070895,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T no-fsync",
            "value": 6986548,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T group-10",
            "value": 36961842,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T group-10",
            "value": 36957820,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T no-fsync",
            "value": 15067686,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T no-fsync",
            "value": 14976347,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T group-10",
            "value": 74999507,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T group-10",
            "value": 74982628,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T no-fsync",
            "value": 31178439,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T no-fsync",
            "value": 30950589,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T group-10",
            "value": 151106215,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T group-10",
            "value": 152043222,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T no-fsync",
            "value": 61903087,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T no-fsync",
            "value": 61919861,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T group-10",
            "value": 301133063,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T group-10",
            "value": 280136306,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-nosync",
            "value": 5155941808,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-fsync",
            "value": 10163447901,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 21264133,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 5254640,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 5151904,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 20554747,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 21252355,
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
          "id": "f232d1515553ce4a50041fabade18e7a41b26571",
          "message": "Page Format Restructure\n\nThis PR modifies the underlying page format in preparation for direct I/O",
          "timestamp": "2025-12-06T00:43:01+05:30",
          "tree_id": "d9c9925182657ab7d98d9c5270e6dcc040494a0b",
          "url": "https://github.com/redixhumayun/simpledb/commit/f232d1515553ce4a50041fabade18e7a41b26571"
        },
        "date": 1764962339768,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 1262,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 4219,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 405428,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 459369,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 703866,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x16 (120 blocks)",
            "value": 956636,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 856176,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 789601,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x16 (1000 ops)",
            "value": 1201044,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 429544,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 1588080,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 1783593,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 442388,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=10, 500 ops)",
            "value": 576935,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 2158717,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=50, 500 ops)",
            "value": 3513922,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 2401655,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=100, 500 ops)",
            "value": 3718528,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 681273,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 732556,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x16 (80/20, 500 ops)",
            "value": 1196727,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 8289512,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 20267475,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 48536079,
            "unit": "ns"
          },
          {
            "name": "Concurrent (16 threads, 1000 ops)",
            "value": 160161287,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 3992533,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 10660435,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (16 threads, K=4, 1000 ops)",
            "value": 21517861,
            "unit": "ns"
          },
          {
            "name": "Sequential Read (1000 blocks)",
            "value": 3223715,
            "unit": "ns"
          },
          {
            "name": "Sequential Write (1000 blocks)",
            "value": 3270198,
            "unit": "ns"
          },
          {
            "name": "Random Read (K=1000, 1000 ops)",
            "value": 3137026,
            "unit": "ns"
          },
          {
            "name": "Random Write (K=1000, 1000 ops)",
            "value": 3261028,
            "unit": "ns"
          },
          {
            "name": "WAL append (no fsync)",
            "value": 128058480,
            "unit": "ns"
          },
          {
            "name": "WAL append + immediate fsync",
            "value": 488206977,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=10)",
            "value": 601182614,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=50)",
            "value": 231386333,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=100)",
            "value": 171013278,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W no-fsync",
            "value": 18518719,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W immediate-fsync",
            "value": 621252757,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W group-10",
            "value": 98002952,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W no-fsync",
            "value": 31155415,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W immediate-fsync",
            "value": 1308549342,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W group-10",
            "value": 156114688,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W no-fsync",
            "value": 57967144,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W immediate-fsync",
            "value": 2225047217,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W group-10",
            "value": 277084911,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T no-fsync",
            "value": 7177263,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T no-fsync",
            "value": 6652645,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T group-10",
            "value": 36941242,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T group-10",
            "value": 35999652,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T no-fsync",
            "value": 15028834,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T no-fsync",
            "value": 15032148,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T group-10",
            "value": 76023997,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T group-10",
            "value": 72669318,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T no-fsync",
            "value": 30892892,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T no-fsync",
            "value": 31001447,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T group-10",
            "value": 151022553,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T group-10",
            "value": 151170174,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T no-fsync",
            "value": 61830765,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T no-fsync",
            "value": 61899812,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T group-10",
            "value": 301105131,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T group-10",
            "value": 300130523,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-nosync",
            "value": 4916782031,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-fsync",
            "value": 8694731430,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 19259644,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 5003504,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 5154033,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 20007498,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 19508461,
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
          "id": "53e5334c0132292a96617005fa669e01e1d0fa6b",
          "message": "Merge pull request #67 from redixhumayun/feature/page-header-restructure\n\nPage Layout Restructure",
          "timestamp": "2025-12-14T20:33:18+05:30",
          "tree_id": "373aad8c29de4a15f62e749653ac4ec1f303dcf9",
          "url": "https://github.com/redixhumayun/simpledb/commit/53e5334c0132292a96617005fa669e01e1d0fa6b"
        },
        "date": 1765724944950,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 961,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 3980,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 454016,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 449475,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 648756,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x16 (120 blocks)",
            "value": 923211,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 886940,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 815524,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x16 (1000 ops)",
            "value": 1207135,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 427986,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 1556853,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 1682726,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 448194,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=10, 500 ops)",
            "value": 586757,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 1999133,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=50, 500 ops)",
            "value": 3196889,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 2285775,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=100, 500 ops)",
            "value": 3572516,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 746558,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 759141,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x16 (80/20, 500 ops)",
            "value": 1039365,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 1000 ops)",
            "value": 7889086,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 1000 ops)",
            "value": 19695866,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1000 ops)",
            "value": 50994185,
            "unit": "ns"
          },
          {
            "name": "Concurrent (16 threads, 1000 ops)",
            "value": 159947188,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 1000 ops)",
            "value": 4053286,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1000 ops)",
            "value": 10438728,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (16 threads, K=4, 1000 ops)",
            "value": 22432693,
            "unit": "ns"
          },
          {
            "name": "Sequential Read (1000 blocks)",
            "value": 3173592,
            "unit": "ns"
          },
          {
            "name": "Sequential Write (1000 blocks)",
            "value": 3331384,
            "unit": "ns"
          },
          {
            "name": "Random Read (K=1000, 1000 ops)",
            "value": 3124793,
            "unit": "ns"
          },
          {
            "name": "Random Write (K=1000, 1000 ops)",
            "value": 3319085,
            "unit": "ns"
          },
          {
            "name": "WAL append (no fsync)",
            "value": 111018804,
            "unit": "ns"
          },
          {
            "name": "WAL append + immediate fsync",
            "value": 485110574,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=10)",
            "value": 507185001,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=50)",
            "value": 183958666,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=100)",
            "value": 132673607,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W no-fsync",
            "value": 16790076,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W immediate-fsync",
            "value": 573294433,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W group-10",
            "value": 60318240,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W no-fsync",
            "value": 24916619,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W immediate-fsync",
            "value": 1185583590,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W group-10",
            "value": 123046279,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W no-fsync",
            "value": 48065928,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W immediate-fsync",
            "value": 2066051549,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W group-10",
            "value": 274715435,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T no-fsync",
            "value": 5896534,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T no-fsync",
            "value": 6038996,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T group-10",
            "value": 29934139,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T group-10",
            "value": 33044992,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T no-fsync",
            "value": 13535609,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T no-fsync",
            "value": 12869818,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T group-10",
            "value": 60782988,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T group-10",
            "value": 61003402,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T no-fsync",
            "value": 29733553,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T no-fsync",
            "value": 31003235,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T group-10",
            "value": 151078233,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T group-10",
            "value": 149374479,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T no-fsync",
            "value": 61833217,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T no-fsync",
            "value": 61982369,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T group-10",
            "value": 303132342,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T group-10",
            "value": 301064990,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-nosync",
            "value": 4960659538,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-fsync",
            "value": 8262048241,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 18806738,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 4797090,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 6603460,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 18008715,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 15801795,
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
          "id": "8990c7a792f6ea02dccc56899202bb23500aaa9e",
          "message": "Merge pull request #68 from redixhumayun/feature/buffer-pool-benchmarks-additions\n\nBuffer Pool Benchmark Updates & Performance Optimizations",
          "timestamp": "2026-01-13T11:10:36+05:30",
          "tree_id": "a1a2389e28941d0b3407d2079f9fe991fbe153c3",
          "url": "https://github.com/redixhumayun/simpledb/commit/8990c7a792f6ea02dccc56899202bb23500aaa9e"
        },
        "date": 1768283260153,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 1122,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 6170,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 836632,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 685411,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x2 (120 blocks)",
            "value": 862259,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 1064304,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x8 (120 blocks)",
            "value": 1050570,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x16 (120 blocks)",
            "value": 1242541,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x32 (120 blocks)",
            "value": 1642766,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x64 (120 blocks)",
            "value": 1807066,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x128 (120 blocks)",
            "value": 1996978,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x256 (120 blocks)",
            "value": 2437887,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 1171101,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x2 (1000 ops)",
            "value": 1163041,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 2101641,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x8 (1000 ops)",
            "value": 2323819,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x16 (1000 ops)",
            "value": 2376486,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x32 (1000 ops)",
            "value": 2461436,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x64 (1000 ops)",
            "value": 2572867,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x128 (1000 ops)",
            "value": 2909223,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x256 (1000 ops)",
            "value": 3544180,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 579753,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 2366619,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 2584614,
            "unit": "ns"
          },
          {
            "name": "Random MT x2 (K=10, 500 ops)",
            "value": 605744,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 975194,
            "unit": "ns"
          },
          {
            "name": "Random MT x8 (K=10, 500 ops)",
            "value": 1136923,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=10, 500 ops)",
            "value": 1200364,
            "unit": "ns"
          },
          {
            "name": "Random MT x32 (K=10, 500 ops)",
            "value": 1273042,
            "unit": "ns"
          },
          {
            "name": "Random MT x64 (K=10, 500 ops)",
            "value": 1426032,
            "unit": "ns"
          },
          {
            "name": "Random MT x128 (K=10, 500 ops)",
            "value": 1752236,
            "unit": "ns"
          },
          {
            "name": "Random MT x256 (K=10, 500 ops)",
            "value": 2248208,
            "unit": "ns"
          },
          {
            "name": "Random MT x2 (K=50, 500 ops)",
            "value": 2061056,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 2898820,
            "unit": "ns"
          },
          {
            "name": "Random MT x8 (K=50, 500 ops)",
            "value": 3447018,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=50, 500 ops)",
            "value": 3908783,
            "unit": "ns"
          },
          {
            "name": "Random MT x32 (K=50, 500 ops)",
            "value": 5810183,
            "unit": "ns"
          },
          {
            "name": "Random MT x64 (K=50, 500 ops)",
            "value": 6888307,
            "unit": "ns"
          },
          {
            "name": "Random MT x128 (K=50, 500 ops)",
            "value": 7617313,
            "unit": "ns"
          },
          {
            "name": "Random MT x256 (K=50, 500 ops)",
            "value": 7963393,
            "unit": "ns"
          },
          {
            "name": "Random MT x2 (K=100, 500 ops)",
            "value": 2380554,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 3452415,
            "unit": "ns"
          },
          {
            "name": "Random MT x8 (K=100, 500 ops)",
            "value": 3916963,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=100, 500 ops)",
            "value": 4522577,
            "unit": "ns"
          },
          {
            "name": "Random MT x32 (K=100, 500 ops)",
            "value": 6931502,
            "unit": "ns"
          },
          {
            "name": "Random MT x64 (K=100, 500 ops)",
            "value": 9204974,
            "unit": "ns"
          },
          {
            "name": "Random MT x128 (K=100, 500 ops)",
            "value": 9104632,
            "unit": "ns"
          },
          {
            "name": "Random MT x256 (K=100, 500 ops)",
            "value": 9572613,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 1043399,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x2 (80/20, 500 ops)",
            "value": 722479,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 996483,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x8 (80/20, 500 ops)",
            "value": 1326071,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x16 (80/20, 500 ops)",
            "value": 1420772,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x32 (80/20, 500 ops)",
            "value": 1608754,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x64 (80/20, 500 ops)",
            "value": 1836889,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x128 (80/20, 500 ops)",
            "value": 2104378,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x256 (80/20, 500 ops)",
            "value": 2659003,
            "unit": "ns"
          },
          {
            "name": "Concurrent (1 threads, 10000 ops)",
            "value": 11914985,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 5000 ops)",
            "value": 11644375,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 2500 ops)",
            "value": 23244709,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1250 ops)",
            "value": 24170203,
            "unit": "ns"
          },
          {
            "name": "Concurrent (16 threads, 625 ops)",
            "value": 23929080,
            "unit": "ns"
          },
          {
            "name": "Concurrent (32 threads, 312 ops)",
            "value": 23485248,
            "unit": "ns"
          },
          {
            "name": "Concurrent (64 threads, 156 ops)",
            "value": 23571862,
            "unit": "ns"
          },
          {
            "name": "Concurrent (128 threads, 78 ops)",
            "value": 23799856,
            "unit": "ns"
          },
          {
            "name": "Concurrent (256 threads, 39 ops)",
            "value": 24458438,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (1 threads, K=4, 10000 ops)",
            "value": 11998004,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (2 threads, K=4, 5000 ops)",
            "value": 10867927,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 2500 ops)",
            "value": 17736078,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1250 ops)",
            "value": 19531242,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (16 threads, K=4, 625 ops)",
            "value": 19613197,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (32 threads, K=4, 312 ops)",
            "value": 19583524,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (64 threads, K=4, 156 ops)",
            "value": 19711695,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (128 threads, K=4, 78 ops)",
            "value": 19886434,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (256 threads, K=4, 39 ops)",
            "value": 20349799,
            "unit": "ns"
          },
          {
            "name": "Sequential Read (1000 blocks)",
            "value": 4287725,
            "unit": "ns"
          },
          {
            "name": "Sequential Write (1000 blocks)",
            "value": 4430660,
            "unit": "ns"
          },
          {
            "name": "Random Read (K=1000, 1000 ops)",
            "value": 4305724,
            "unit": "ns"
          },
          {
            "name": "Random Write (K=1000, 1000 ops)",
            "value": 4447707,
            "unit": "ns"
          },
          {
            "name": "WAL append (no fsync)",
            "value": 221717779,
            "unit": "ns"
          },
          {
            "name": "WAL append + immediate fsync",
            "value": 863400186,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=10)",
            "value": 1054425519,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=50)",
            "value": 402647102,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=100)",
            "value": 304314298,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W no-fsync",
            "value": 30730523,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W immediate-fsync",
            "value": 1176184378,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W group-10",
            "value": 146630453,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W no-fsync",
            "value": 57342577,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W immediate-fsync",
            "value": 2295259046,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W group-10",
            "value": 256820604,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W no-fsync",
            "value": 97056273,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W immediate-fsync",
            "value": 3876014101,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W group-10",
            "value": 488782786,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T no-fsync",
            "value": 15156008,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T no-fsync",
            "value": 13204837,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T group-10",
            "value": 67985900,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T group-10",
            "value": 65540008,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T no-fsync",
            "value": 32148648,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T no-fsync",
            "value": 30007115,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T group-10",
            "value": 143035819,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T group-10",
            "value": 129370463,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T no-fsync",
            "value": 61798617,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T no-fsync",
            "value": 56599581,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T group-10",
            "value": 270532915,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T group-10",
            "value": 285375938,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T no-fsync",
            "value": 121508870,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T no-fsync",
            "value": 115892098,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T group-10",
            "value": 529050358,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T group-10",
            "value": 547054684,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-nosync",
            "value": 8673596971,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-fsync",
            "value": 11952159813,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 35154271,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 8838976,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 8634538,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 34252404,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 34420492,
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
          "id": "65a7d7856ad7140c0bf2270a1cc3b2bcf8d7b612",
          "message": "Merge pull request #70 from redixhumayun/feature/wal-logging\n\nWAL Logging",
          "timestamp": "2026-02-16T18:48:27-08:00",
          "tree_id": "9038c2a190483c47456e5d5511635918a2f3301c",
          "url": "https://github.com/redixhumayun/simpledb/commit/65a7d7856ad7140c0bf2270a1cc3b2bcf8d7b612"
        },
        "date": 1771296945010,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Pin/Unpin (hit)",
            "value": 1120,
            "unit": "ns"
          },
          {
            "name": "Cold Pin (miss)",
            "value": 6196,
            "unit": "ns"
          },
          {
            "name": "Dirty Eviction",
            "value": 884009,
            "unit": "ns"
          },
          {
            "name": "Sequential Scan (120 blocks)",
            "value": 686966,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x2 (120 blocks)",
            "value": 821063,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x4 (120 blocks)",
            "value": 866713,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x8 (120 blocks)",
            "value": 1057035,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x16 (120 blocks)",
            "value": 1244890,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x32 (120 blocks)",
            "value": 1652410,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x64 (120 blocks)",
            "value": 1868301,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x128 (120 blocks)",
            "value": 2027830,
            "unit": "ns"
          },
          {
            "name": "Seq Scan MT x256 (120 blocks)",
            "value": 2569488,
            "unit": "ns"
          },
          {
            "name": "Repeated Access (1000 ops)",
            "value": 1189018,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x2 (1000 ops)",
            "value": 1163257,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x4 (1000 ops)",
            "value": 2045146,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x8 (1000 ops)",
            "value": 2320392,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x16 (1000 ops)",
            "value": 2385851,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x32 (1000 ops)",
            "value": 2464435,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x64 (1000 ops)",
            "value": 2590208,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x128 (1000 ops)",
            "value": 2902163,
            "unit": "ns"
          },
          {
            "name": "Repeated Access MT x256 (1000 ops)",
            "value": 3583400,
            "unit": "ns"
          },
          {
            "name": "Random (K=10, 500 ops)",
            "value": 588731,
            "unit": "ns"
          },
          {
            "name": "Random (K=50, 500 ops)",
            "value": 2411839,
            "unit": "ns"
          },
          {
            "name": "Random (K=100, 500 ops)",
            "value": 2657530,
            "unit": "ns"
          },
          {
            "name": "Random MT x2 (K=10, 500 ops)",
            "value": 606930,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=10, 500 ops)",
            "value": 983355,
            "unit": "ns"
          },
          {
            "name": "Random MT x8 (K=10, 500 ops)",
            "value": 1124490,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=10, 500 ops)",
            "value": 1193541,
            "unit": "ns"
          },
          {
            "name": "Random MT x32 (K=10, 500 ops)",
            "value": 1277243,
            "unit": "ns"
          },
          {
            "name": "Random MT x64 (K=10, 500 ops)",
            "value": 1434624,
            "unit": "ns"
          },
          {
            "name": "Random MT x128 (K=10, 500 ops)",
            "value": 1743551,
            "unit": "ns"
          },
          {
            "name": "Random MT x256 (K=10, 500 ops)",
            "value": 2220937,
            "unit": "ns"
          },
          {
            "name": "Random MT x2 (K=50, 500 ops)",
            "value": 1944411,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=50, 500 ops)",
            "value": 2896313,
            "unit": "ns"
          },
          {
            "name": "Random MT x8 (K=50, 500 ops)",
            "value": 3408441,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=50, 500 ops)",
            "value": 3957679,
            "unit": "ns"
          },
          {
            "name": "Random MT x32 (K=50, 500 ops)",
            "value": 5726746,
            "unit": "ns"
          },
          {
            "name": "Random MT x64 (K=50, 500 ops)",
            "value": 7037459,
            "unit": "ns"
          },
          {
            "name": "Random MT x128 (K=50, 500 ops)",
            "value": 7387916,
            "unit": "ns"
          },
          {
            "name": "Random MT x256 (K=50, 500 ops)",
            "value": 7741988,
            "unit": "ns"
          },
          {
            "name": "Random MT x2 (K=100, 500 ops)",
            "value": 2295139,
            "unit": "ns"
          },
          {
            "name": "Random MT x4 (K=100, 500 ops)",
            "value": 3390411,
            "unit": "ns"
          },
          {
            "name": "Random MT x8 (K=100, 500 ops)",
            "value": 3980873,
            "unit": "ns"
          },
          {
            "name": "Random MT x16 (K=100, 500 ops)",
            "value": 4539999,
            "unit": "ns"
          },
          {
            "name": "Random MT x32 (K=100, 500 ops)",
            "value": 6864037,
            "unit": "ns"
          },
          {
            "name": "Random MT x64 (K=100, 500 ops)",
            "value": 8947425,
            "unit": "ns"
          },
          {
            "name": "Random MT x128 (K=100, 500 ops)",
            "value": 8787390,
            "unit": "ns"
          },
          {
            "name": "Random MT x256 (K=100, 500 ops)",
            "value": 9128794,
            "unit": "ns"
          },
          {
            "name": "Zipfian (80/20, 500 ops)",
            "value": 991963,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x2 (80/20, 500 ops)",
            "value": 760532,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x4 (80/20, 500 ops)",
            "value": 958598,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x8 (80/20, 500 ops)",
            "value": 1167357,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x16 (80/20, 500 ops)",
            "value": 1434746,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x32 (80/20, 500 ops)",
            "value": 1708869,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x64 (80/20, 500 ops)",
            "value": 1681830,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x128 (80/20, 500 ops)",
            "value": 2084315,
            "unit": "ns"
          },
          {
            "name": "Zipfian MT x256 (80/20, 500 ops)",
            "value": 2746945,
            "unit": "ns"
          },
          {
            "name": "Concurrent (1 threads, 10000 ops)",
            "value": 11678924,
            "unit": "ns"
          },
          {
            "name": "Concurrent (2 threads, 5000 ops)",
            "value": 11758178,
            "unit": "ns"
          },
          {
            "name": "Concurrent (4 threads, 2500 ops)",
            "value": 22547651,
            "unit": "ns"
          },
          {
            "name": "Concurrent (8 threads, 1250 ops)",
            "value": 24630768,
            "unit": "ns"
          },
          {
            "name": "Concurrent (16 threads, 625 ops)",
            "value": 23955178,
            "unit": "ns"
          },
          {
            "name": "Concurrent (32 threads, 312 ops)",
            "value": 23617876,
            "unit": "ns"
          },
          {
            "name": "Concurrent (64 threads, 156 ops)",
            "value": 23584198,
            "unit": "ns"
          },
          {
            "name": "Concurrent (128 threads, 78 ops)",
            "value": 23759023,
            "unit": "ns"
          },
          {
            "name": "Concurrent (256 threads, 39 ops)",
            "value": 24520539,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (1 threads, K=4, 10000 ops)",
            "value": 11906621,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (2 threads, K=4, 5000 ops)",
            "value": 11100432,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (4 threads, K=4, 2500 ops)",
            "value": 17805453,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (8 threads, K=4, 1250 ops)",
            "value": 19407087,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (16 threads, K=4, 625 ops)",
            "value": 19500622,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (32 threads, K=4, 312 ops)",
            "value": 19616460,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (64 threads, K=4, 156 ops)",
            "value": 19666832,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (128 threads, K=4, 78 ops)",
            "value": 19884827,
            "unit": "ns"
          },
          {
            "name": "Concurrent Hotset (256 threads, K=4, 39 ops)",
            "value": 20217580,
            "unit": "ns"
          },
          {
            "name": "Sequential Read (1000 blocks)",
            "value": 4380378,
            "unit": "ns"
          },
          {
            "name": "Sequential Write (1000 blocks)",
            "value": 4514020,
            "unit": "ns"
          },
          {
            "name": "Random Read (K=1000, 1000 ops)",
            "value": 4408012,
            "unit": "ns"
          },
          {
            "name": "Random Write (K=1000, 1000 ops)",
            "value": 4539940,
            "unit": "ns"
          },
          {
            "name": "WAL append (no fsync)",
            "value": 240311003,
            "unit": "ns"
          },
          {
            "name": "WAL append + immediate fsync",
            "value": 857047819,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=10)",
            "value": 1091248075,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=50)",
            "value": 400183278,
            "unit": "ns"
          },
          {
            "name": "WAL group commit (batch=100)",
            "value": 300658973,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W no-fsync",
            "value": 37046546,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W immediate-fsync",
            "value": 1319053121,
            "unit": "ns"
          },
          {
            "name": "Mixed 70/30R/W group-10",
            "value": 158686289,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W no-fsync",
            "value": 62651344,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W immediate-fsync",
            "value": 2188665859,
            "unit": "ns"
          },
          {
            "name": "Mixed 50/50R/W group-10",
            "value": 274143599,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W no-fsync",
            "value": 102358634,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W immediate-fsync",
            "value": 3920992810,
            "unit": "ns"
          },
          {
            "name": "Mixed 10/90R/W group-10",
            "value": 495687714,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T no-fsync",
            "value": 16756812,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T no-fsync",
            "value": 14292391,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 2T group-10",
            "value": 65770523,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 2T group-10",
            "value": 69108023,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T no-fsync",
            "value": 32222570,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T no-fsync",
            "value": 29848851,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 4T group-10",
            "value": 139745018,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 4T group-10",
            "value": 126778821,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T no-fsync",
            "value": 59999581,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T no-fsync",
            "value": 58561788,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 8T group-10",
            "value": 272210325,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 8T group-10",
            "value": 257261312,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T no-fsync",
            "value": 124011331,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T no-fsync",
            "value": 114676504,
            "unit": "ns"
          },
          {
            "name": "Concurrent shared 16T group-10",
            "value": 544814980,
            "unit": "ns"
          },
          {
            "name": "Concurrent sharded 16T group-10",
            "value": 523512874,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-nosync",
            "value": 8649228165,
            "unit": "ns"
          },
          {
            "name": "Random Write durability immediate-fsync data-fsync",
            "value": 12329735872,
            "unit": "ns"
          },
          {
            "name": "INSERT (single record)",
            "value": 38422242,
            "unit": "ns"
          },
          {
            "name": "SELECT (table scan)",
            "value": 8926887,
            "unit": "ns"
          },
          {
            "name": "SELECT COUNT(*)",
            "value": 9047311,
            "unit": "ns"
          },
          {
            "name": "UPDATE (single record)",
            "value": 37943710,
            "unit": "ns"
          },
          {
            "name": "DELETE (single record)",
            "value": 36612941,
            "unit": "ns"
          }
        ]
      }
    ]
  }
}